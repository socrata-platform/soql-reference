package com.socrata.soql

import java.io.InputStream

import com.google.protobuf.CodedInputStream
import com.socrata.NonEmptySeq
import com.socrata.soql.ast.JoinType
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.functions.{Function, MonomorphicFunction}
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.typed._
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

class AnalysisDeserializer[C, T](columnDeserializer: String => C, typeDeserializer: String => T, functionMap: String => Function[T]) extends (InputStream => NonEmptySeq[SoQLAnalysis[C, T]]) {
  import AnalysisDeserializer._

  type Expr = CoreExpr[C, T]
  type Order = OrderBy[C, T]

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
                             dictionary: DeserializationDictionary[C, T],
                             version: Int)
  {
    // TODO: remove after release of 2.10.6 or higher (exists for 2.10.5- backwards-compatibility during release process)
    val isV4 = version == 4 || version == TestVersionV4

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
          ColumnRef(qual, name, typ)(pos)
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
          val window = readWindowFunctionInfo()
          FunctionCall(func, params, window)(pos, functionNamePosition)
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

    def readJoins(): Seq[Join[C, T]] = {
      readSeq {
        val joinType = JoinType(in.readString())
        val joinAnalysis =
          if (isV4) {
            val tableLikeAndFroms = readWithFrom().seq
            val from = tableLikeAndFroms.head._2.get // must exist by conventions of v4
            val tableLike = tableLikeAndFroms.map(_._1)
            val alias = maybeRead(in.readString)
            v4JoinToV5Join(from, tableLike, alias)
          } else {
            readJoinAnalysis()
          }
        Join(joinType, joinAnalysis, readExpr())
      }
    }

    def readWindowFunctionInfo(): Option[WindowFunctionInfo[C, T]] = {
      readSeq {
        val partitions = readSeq { readExpr() }
        val orderings = readOrderBy()
        val frames = readSeq { readExpr() }
        WindowFunctionInfo(partitions, orderings, frames)
      }.headOption
    }

    // TODO: remove after release of 2.10.6 or higher (for 2.10.5- backwards-compatibility)
    def v4JoinToV5Join(from: String, tableLike: Seq[SoQLAnalysis[C, T]], aliasOpt: Option[String]): JoinAnalysis[C, T] = {
      val subAnaOpt = if (tableLike.size == 1 && tableLike.head.selection.keys.isEmpty) {
        None
      } else {
        val subAnalyses = NonEmptySeq.fromSeq(tableLike)
        val alias = aliasOpt.getOrElse {
          throw new RuntimeException(s"alias must be present in a sub-select (join, from: $from)")
        }
        subAnalyses.map(SubAnalysis(_, alias))
      }

      val tableNameAlias = if (subAnaOpt.isEmpty) aliasOpt else None

      JoinAnalysis(TableName(from, tableNameAlias), subAnaOpt)
    }

    def readWhere(): Option[Expr] = maybeRead { readExpr() }

    def readHaving(): Option[Expr] = readWhere()

    // backwards compatible (v4) sequence reading (used to be Option[Seq[T]] (v4) instead of Seq[T] (v5))
    def bcSeq[T](f: => Seq[T]): Seq[T] = {
      if (isV4) {
        maybeRead(f).toSeq.flatten
      } else {
        f
      }
    }

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

    def readSubAnalysis(): SubAnalysis[C, T] = {
      SubAnalysis(read(), in.readString())
    }

    def readJoinAnalysis(): JoinAnalysis[C, T] = {
      JoinAnalysis(readTableName(), maybeRead(readSubAnalysis()))
    }

    def readSearch(): Option[String] =
      maybeRead {
        dictionary.strings(in.readUInt32())
      }

    def readAnaAndFrom(): (SoQLAnalysis[C, T], Option[String]) = {
      lazy val fromOptTrigger = if (isV4) maybeRead(in.readString) else None

      val ig = readIsGrouped()
      val d = readDistinct()
      val s = readSelection()
      val j = {
        fromOptTrigger; // trigger eval, read "from" string if v4
        readJoins()
      }
      val w = readWhere()
      val gb = bcSeq { readGroupBy() }
      val h = readHaving()
      val ob = bcSeq { readOrderBy() }
      val l = readLimit()
      val o = readOffset()
      val search = readSearch()

      val ana = SoQLAnalysis(ig, d, s, j, w, gb, h, ob, l, o, search)

      (ana, fromOptTrigger)
    }

    def readAnalysis(): SoQLAnalysis[C, T] = {
      readAnaAndFrom()._1
    }

    def read(): NonEmptySeq[SoQLAnalysis[C, T]] = {
      readNonEmptySeq { readAnalysis() }
    }

    def readWithFrom(): NonEmptySeq[(SoQLAnalysis[C, T], Option[String])] = {
      readNonEmptySeq { readAnaAndFrom() }
    }
  }

  def apply(in: InputStream): NonEmptySeq[SoQLAnalysis[C, T]] = {
    val cis = CodedInputStream.newInstance(in)
    cis.readInt32() match {
      case v if v == TestVersionV4 || v == TestVersionV5 => // single select
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary, v)
        NonEmptySeq(deserializer.readAnalysis(), Seq.empty)
      case v if v >= 3 && v <= 4 =>
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary, v)
        deserializer.read()
      case v@CurrentVersion =>
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary, v)
        deserializer.read()
      case other =>
        throw new UnknownAnalysisSerializationVersion(other)
    }
  }
}
