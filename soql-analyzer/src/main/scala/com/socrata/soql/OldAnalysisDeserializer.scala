package com.socrata.soql.oldanalysis

import java.io.InputStream

import com.google.protobuf.CodedInputStream
import com.socrata.soql.ast.{JoinType, InnerJoinType, LeftOuterJoinType, RightOuterJoinType, FullOuterJoinType}
import com.socrata.soql.collection.{OrderedMap, NonEmptySeq}
import com.socrata.soql.collection.SeqHelpers._
import com.socrata.soql.environment.{ColumnName, Qualified, TableRef, ResourceName}
import com.socrata.soql.functions.{Function, MonomorphicFunction}
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.typed._
import com.socrata.soql.{SoQLAnalysis, JoinAnalysis}
import gnu.trove.map.hash.TIntObjectHashMap

import scala.util.parsing.input.{NoPosition, Position}

private case class SimplePosition(line: Int, column: Int, lineContents: String) extends Position

class UnknownAnalysisSerializationVersion(val version: Int) extends Exception("Unknown analysis serialization version " + version)

private trait DeserializationDictionary[C, T] {
  def types(i: Int): T
  def labels(i: Int): ColumnName
  def columns(i: Int): C
  def functions(i: Int): MonomorphicFunction[T]
  def strings(i: Int): String
}

object AnalysisDeserializer {
  val CurrentVersion = 5

  // This is odd and for smooth deploy transition.
  val TestVersionV4 = 0
  val TestVersionV5 = -1
}

class AnalysisDeserializer[C, T](columnDeserializer: String => C, typeDeserializer: String => T, functionMap: String => Function[T]) {
  import AnalysisDeserializer._

  type Expr = CoreExpr[OldQ, T]
  type Order = OrderBy[OldQ, T]

  private class DeserializationDictionaryImpl(typesRegistry: TIntObjectHashMap[T],
                                      stringsRegistry: TIntObjectHashMap[String],
                                      labelsRegistry: TIntObjectHashMap[ColumnName],
                                      columnsRegistry: TIntObjectHashMap[C],
                                      functionsRegistry: TIntObjectHashMap[MonomorphicFunction[T]])
    extends DeserializationDictionary[C, T]
  {
    def types(i: Int): T = typesRegistry.get(i)
    def strings(i: Int): String = stringsRegistry.get(i)
    def labels(i: Int): ColumnName = labelsRegistry.get(i)
    def columns(i: Int): C = columnsRegistry.get(i)
    def functions(i: Int): MonomorphicFunction[T] = functionsRegistry.get(i)
  }

  case class OldQ(qual: Option[String], c: C)
  case class TableName(name: String, alias: Option[String])
  case class OldSubAnalysis[ColumnId, Type](analyses: NonEmptySeq[OldSoQLAnalysis[ColumnId, Type]], alias: String)
  case class OldJoinAnalysis[ColumnId, Type](fromTable: TableName, subAnalysis: Option[OldSubAnalysis[ColumnId, Type]])
  case class OldJoin[ColumnId, Type](joinType: JoinType, from: OldJoinAnalysis[ColumnId, Type], on: CoreExpr[ColumnId, Type])

  case class OldSoQLAnalysis[ColumnId, Type](isGrouped: Boolean,
                                             distinct: Boolean,
                                             selection: OrderedMap[ColumnName, CoreExpr[ColumnId, Type]],
                                             joins: Seq[OldJoin[ColumnId, Type]],
                                             where: Option[CoreExpr[ColumnId, Type]],
                                             groupBys: Seq[CoreExpr[ColumnId, Type]],
                                             having: Option[CoreExpr[ColumnId, Type]],
                                             orderBys: Seq[OrderBy[ColumnId, Type]],
                                             limit: Option[BigInt],
                                             offset: Option[BigInt],
                                             search: Option[String])

  private object DeserializationDictionaryImpl {
    private def readRegistry[A](in: CodedInputStream)(f: => A): TIntObjectHashMap[A] = {
      val result = new TIntObjectHashMap[A]
      (1 to in.readUInt32()).foreach { _ =>
        val a = f
        val i = in.readUInt32()
        result.put(i, a)
      }
      result
    }

    def fromInput(in: CodedInputStream): DeserializationDictionary[C, T] = {
      val strings = readRegistry(in) {
        in.readString()
      }
      val labels = readRegistry(in) {
        ColumnName(strings.get(in.readUInt32()))
      }
      val types = readRegistry(in) {
        typeDeserializer(strings.get(in.readUInt32()))
      }
      val columns = readRegistry(in) {
        columnDeserializer(strings.get(in.readUInt32()))
      }
      val functions = readRegistry(in) {
        val function = functionMap(strings.get(in.readUInt32()))
        val bindingsBuilder = Map.newBuilder[String, T]
        val bindings = (1 to in.readUInt32()).map { _ =>
          val typeVar = strings.get(in.readUInt32())
          val typ = types.get(in.readUInt32())
          typeVar -> typ
        }.toMap
        MonomorphicFunction(function, bindings)
      }

      new DeserializationDictionaryImpl(types, strings, labels, columns, functions)
    }
  }

  private class Deserializer(in: CodedInputStream,
                             dictionary: DeserializationDictionary[C, T])
  {
    def readPosition(): Position =
      in.readRawByte() match {
        case 0 =>
          val line = in.readUInt32()
          val column = in.readUInt32()
          val sourceText = dictionary.strings(in.readUInt32())
          val offset = in.readUInt32()
          SoQLPosition(line, column, sourceText, offset)
        case 1 =>
          NoPosition
        case 2 =>
          val line = in.readUInt32()
          val column = in.readUInt32()
          val lineText = dictionary.strings(in.readUInt32())
          SimplePosition(line, column, lineText)
      }

    def readExpr(): Expr = {
      val pos = readPosition()
      in.readRawByte() match {
        case 1 =>
          val qual = maybeRead(in.readString())
          val name = dictionary.columns(in.readUInt32())
          val typ = dictionary.types(in.readUInt32())
          ColumnRef(OldQ(qual, name), typ)(pos)
        case 2 =>
          val value = dictionary.strings(in.readUInt32())
          val typ = dictionary.types(in.readUInt32())
          StringLiteral(value, typ)(pos)
        case 3 =>
          val value = BigDecimal(dictionary.strings(in.readUInt32()))
          val typ = dictionary.types(in.readUInt32())
          NumberLiteral(value, typ)(pos)
        case 4 =>
          val value = in.readBool()
          val typ = dictionary.types(in.readUInt32())
          BooleanLiteral(value, typ)(pos)
        case 5 =>
          val typ = dictionary.types(in.readUInt32())
          NullLiteral(typ)(pos)
        case 6 =>
          val functionNamePosition = readPosition()
          val func = dictionary.functions(in.readUInt32())
          val params = readSeq { readExpr() }
          FunctionCall(func, params)(pos, functionNamePosition)
      }
    }

    def readIsGrouped(): Boolean = in.readBool()

    def readDistinct(): Boolean = in.readBool()

    def maybeRead[A](f: => A): Option[A] = {
      if (in.readBool()) Some(f)
      else None
    }

    def readSeq[A](f: => A): Seq[A] = {
      val count = in.readUInt32()
      1.to(count).map { _ => f }
    }

    def readNonEmptySeq[A](f: => A): NonEmptySeq[A] = NonEmptySeq.fromSeqUnsafe(readSeq(f))

    def readSelection(): OrderedMap[ColumnName, Expr] = {
      val elems = readSeq {
        val name =  dictionary.labels(in.readUInt32())
        val expr = readExpr()
        name -> expr
      }
      OrderedMap(elems: _*)
    }

    def readJoins(): Seq[OldJoin[OldQ, T]] = {
      readSeq {
        val joinType = in.readString() match {
          case JoinType.InnerJoinName => InnerJoinType
          case JoinType.LeftOuterJoinName => LeftOuterJoinType
          case JoinType.RightOuterJoinName => RightOuterJoinType
          case JoinType.FullOuterJoinName => FullOuterJoinType
        }
        val joinAnalysis = readJoinAnalysis()
        OldJoin(joinType, joinAnalysis, readExpr())
      }
    }

    def readWhere(): Option[Expr] = maybeRead { readExpr() }

    def readHaving(): Option[Expr] = readWhere()

    def readGroupBy(): Seq[Expr] = readSeq { readExpr() }

    def readOrderBy(): Seq[Order] =
      readSeq {
        val expr = readExpr()
        val ascending = in.readBool()
        val nullsLast = in.readBool()
        OrderBy(expr, ascending, nullsLast)
      }

    def readLimit(): Option[BigInt] =
      maybeRead {
        BigInt(dictionary.strings(in.readUInt32()))
      }

    def readOffset(): Option[BigInt] = readLimit()

    def readTableName(): TableName = {
      TableName(in.readString(), maybeRead { in.readString() })
    }

    def readSubAnalysis(): OldSubAnalysis[OldQ, T] = {
      OldSubAnalysis(read(), in.readString())
    }

    def readJoinAnalysis(): OldJoinAnalysis[OldQ, T] = {
      OldJoinAnalysis(readTableName, maybeRead(readSubAnalysis()))
    }

    def readSearch(): Option[String] =
      maybeRead {
        dictionary.strings(in.readUInt32())
      }

    def readAnalysis(): OldSoQLAnalysis[OldQ, T] = {
      val ig = readIsGrouped()
      val d = readDistinct()
      val s = readSelection()
      val j = readJoins()
      val w = readWhere()
      val gb = readGroupBy()
      val h = readHaving()
      val ob = readOrderBy()
      val l = readLimit()
      val o = readOffset()
      val search = readSearch()

      val ana = OldSoQLAnalysis(ig, d, s, j, w, gb, h, ob, l, o, search)

      ana
    }

    def read(): NonEmptySeq[OldSoQLAnalysis[OldQ, T]] = {
      readNonEmptySeq { readAnalysis() }
    }
  }

  case class ConvertState(prefixRefs: Map[Option[String], TableRef], aliases: Map[String, String], subselectCtr: Int)

  def convertExpr(state: ConvertState, expr: CoreExpr[OldQ, T]): CoreExpr[Qualified[C], T] =
    expr match {
      case cr@ColumnRef(qname, typ) =>
        ColumnRef(Qualified(state.prefixRefs(qname.qual.map { q => state.aliases.getOrElse(q, q) }), qname.c), typ)(cr.position)
      case lit@NumberLiteral(n, t) =>
        NumberLiteral(n, t)(lit.position)
      case lit@StringLiteral(s, t) =>
        StringLiteral(s, t)(lit.position)
      case lit@BooleanLiteral(b, t) =>
        BooleanLiteral(b, t)(lit.position)
      case lit@NullLiteral(t) =>
        NullLiteral(t)(lit.position)
      case fc@FunctionCall(f, params) =>
        FunctionCall(f, params.map(convertExpr(state, _)))(fc.position, fc.functionNamePosition)
    }

  val FourFourish = """_([a-kmnp-z2-9]{4}-[a-kmnp-z2-9]{4})""".r
  def convertFourFourish(s: String) =
    s match {
      case FourFourish(fourfour) => fourfour
      case other => other
    }

  def convertJoinAnalysis(state: ConvertState, ja: OldJoinAnalysis[OldQ, T]): (ConvertState, JoinAnalysis[Qualified[C], T]) = {
    val newPrimary = ResourceName(convertFourFourish(ja.fromTable.name))
    val newJoinNum = state.subselectCtr
    val tableAlias = ja.fromTable.alias.getOrElse(ja.fromTable.name)
    ja.subAnalysis match {
      case None =>
        // "join @foo [as bar]" type - add bar/foo to the state and continue
        val analysis = JoinAnalysis[Qualified[C], T](newPrimary, newJoinNum, Nil)
        (ConvertState(state.prefixRefs + (Some(ja.fromTable.name) -> analysis.outputTable),
                      state.aliases + (tableAlias -> ja.fromTable.name),
                      state.subselectCtr + 1),
         analysis)
      case Some(OldSubAnalysis(analyses, resultAlias)) =>
        // "join (select ...) as bar" type - recursively covert analysis
        val (joinState, joinAnalysis) = convert(ConvertState(Map(None -> TableRef.JoinPrimary(newPrimary, newJoinNum),
                                                                 Some(ja.fromTable.name) -> TableRef.JoinPrimary(newPrimary, newJoinNum)),
                                                             Map(tableAlias -> ja.fromTable.name),
                                                             newJoinNum),
                                                analyses)
        val analysis = JoinAnalysis(newPrimary, newJoinNum, joinAnalysis.seq)
        val resultRef = TableRef.Join(joinState.subselectCtr)
        (ConvertState(state.prefixRefs + (Some(resultAlias) -> resultRef),
                      state.aliases,
                      joinState.subselectCtr + 1),
         analysis)
    }
  }

  def convertJoin(state: ConvertState, join: OldJoin[OldQ, T]): (ConvertState, Join[Qualified[C], T]) = {
    val (augmentedState, joinAnalysis) = convertJoinAnalysis(state, join.from)
    (augmentedState, Join(join.joinType, joinAnalysis, convertExpr(augmentedState, join.on)))
  }

  def convertOrderBy(state: ConvertState, ob: OrderBy[OldQ, T]): OrderBy[Qualified[C], T] =
    ob.copy(expression = convertExpr(state, ob.expression))

  def convert1(state: ConvertState, step: OldSoQLAnalysis[OldQ, T]): (ConvertState, SoQLAnalysis[Qualified[C], T]) = {
    val OldSoQLAnalysis(isGrouped, distinct, selection, joins, where, groupBys, having, orderBys, limit, offset, search) = step

    val (intermediateState, newJoins) = joins.mapAccum(state)(convertJoin)

    val newSelection = selection.mapValues(convertExpr(intermediateState, _))

    (intermediateState.copy(prefixRefs = Map(None -> TableRef.PreviousChainStep) ++
                                             // this is because the old system didn't keep close tabs on where columns came from
                                             intermediateState.prefixRefs.mapValues(_ => TableRef.PreviousChainStep)),
     SoQLAnalysis(isGrouped,
                  distinct,
                  newSelection,
                  newJoins,
                  where.map(convertExpr(intermediateState, _)),
                  groupBys.map(convertExpr(intermediateState, _)),
                  having.map(convertExpr(intermediateState, _)),
                  orderBys.map(convertOrderBy(intermediateState, _)),
                  limit,
                  offset,
                  search))
  }

  def convert(initialState: ConvertState, in: NonEmptySeq[OldSoQLAnalysis[OldQ, T]]): (ConvertState, NonEmptySeq[SoQLAnalysis[Qualified[C], T]]) = {
    in.mapAccum(initialState)(convert1)
  }

  def apply(cis: CodedInputStream): NonEmptySeq[SoQLAnalysis[Qualified[C], T]] = {
    val dictionary = DeserializationDictionaryImpl.fromInput(cis)
    val deserializer = new Deserializer(cis, dictionary)
    val old = deserializer.read()
    convert(ConvertState(Map(None -> TableRef.Primary), Map.empty, 0), old)._2
  }
}
