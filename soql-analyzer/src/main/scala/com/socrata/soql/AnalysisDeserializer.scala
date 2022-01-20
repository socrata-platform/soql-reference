package com.socrata.soql

import java.io.InputStream

import gnu.trove.map.hash.TIntObjectHashMap
import com.google.protobuf.CodedInputStream
import com.socrata.NonEmptySeq
import com.socrata.soql.ast.JoinType
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.functions.{Function, MonomorphicFunction}
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.typed._

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
  val CurrentVersion = 10
  val NonEmptySeqVersion = 6

  // This is odd and for smooth deploy transition.
  val TestVersionV5 = -1
}

class AnalysisDeserializer[C, T](columnDeserializer: String => C, typeDeserializer: String => T, functionMap: String => Function[T]) extends (InputStream => NonEmptySeq[SoQLAnalysis[C, T]]) {
  import AnalysisDeserializer._

  type Expr = CoreExpr[C, T]
  type Order = OrderBy[C, T]
  type THint = Hint[C, T]

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
          val filter = maybeRead { readExpr() }
          val window = readWindowFunctionInfo()
          FunctionCall(func, params, filter, window)(pos, functionNamePosition)
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

    def readBinaryTree[A](f: => A): BinaryTree[A] = {
      in.readUInt32() match {
        case 1 =>
          Leaf(f)
        case 2 =>
          val op = in.readString()
          val l = readBinaryTree(f)
          val r = readBinaryTree(f)
          Compound(op, l, r)
      }
    }

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
        val joinAnalysis = readJoinAnalysis()
        val lateral = in.readBool()
        Join(joinType, joinAnalysis, readExpr(), lateral)
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

    def readSubAnalysis(): SubAnalysis[C, T] = {
      if (this.version > NonEmptySeqVersion || this.version == TestVersionV5) {
        SubAnalysis(readBinaryTree(readAnalysis()), in.readString())
      } else {
        val neseq = readNonEmptySeq(readAnalysis())
        val bt = toBinaryTree(neseq.seq)
        SubAnalysis(bt, in.readString())
      }
    }

    /**
     * TODO: For deploy transition only.  To be removed
     */
    private def v6JoinSubAnalysisTov7(subAnalysis: SubAnalysis[C, T], tableName: TableName): SubAnalysis[C, T] = {
      val leftMost = subAnalysis.analyses.leftMost
      val leftMostWithFrom = Leaf(leftMost.leaf.copy(from = Option(tableName)))
      val analysesWithFrom = subAnalysis.analyses.replace(leftMost, leftMostWithFrom)
      subAnalysis.copy(analyses = analysesWithFrom)
    }

    def readJoinAnalysis(): JoinAnalysis[C, T] = {
      if (this.version > NonEmptySeqVersion || this.version == TestVersionV5) {
        in.readUInt32() match {
          case 0 =>
            JoinAnalysis(Left(readTableName()))
          case 1 =>
            JoinAnalysis(Right(readSubAnalysis()))
        }
      } else {
        val tableName = readTableName()
        val subAnalysisOpt = maybeRead(readSubAnalysis())
        subAnalysisOpt match {
          case Some(subAnalysis) =>
            // serialization v7 join.from is replaced with select.from
            // move v6 join.from to select.from
            JoinAnalysis(Right(v6JoinSubAnalysisTov7(subAnalysis, tableName)))
          case None => JoinAnalysis(Left(tableName))
        }
      }
    }

    def readSearch(): Option[String] =
      maybeRead {
        dictionary.strings(in.readUInt32())
      }

    def readFrom(): Option[TableName] =
      maybeRead {
        readTableName()
      }

    def readHint(): Seq[THint] =
      readSeq {
        val pos = readPosition()
        in.readRawByte() match {
          case 1 =>
            Materialized(pos)
          case 2 =>
            UniqueOrder(pos)
        }
      }

    def readAnalysis(): SoQLAnalysis[C, T] = {
      val ig = readIsGrouped()
      val d = readDistinct()
      val s = readSelection()
      val f = if (this.version > NonEmptySeqVersion || this.version == TestVersionV5) readFrom() else None
      val j = readJoins()
      val w = readWhere()
      val gb = readGroupBy()
      val h = readHaving()
      val ob = readOrderBy()
      val l = readLimit()
      val o = readOffset()
      val search = readSearch()
      val hi = if (this.version != (CurrentVersion - 1)) readHint() else Seq.empty

      SoQLAnalysis(ig, d, s, f, j, w, gb, h, ob, l, o, search, hi)
    }

    def read(): NonEmptySeq[SoQLAnalysis[C, T]] = {
      readNonEmptySeq { readAnalysis() }
    }
  }

  def apply(in: InputStream): NonEmptySeq[SoQLAnalysis[C, T]] = {
    val cis = CodedInputStream.newInstance(in)
    cis.readInt32() match {
      case v if v >= 5 && v <= CurrentVersion =>
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary, v)
        deserializer.read()
      case v if v == TestVersionV5 => // single select
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary, v)
        NonEmptySeq(deserializer.readAnalysis(), Seq.empty)
      case other =>
        throw new UnknownAnalysisSerializationVersion(other)
    }
  }

  def applyBinaryTree(in: InputStream): BinaryTree[SoQLAnalysis[C, T]] = {
    val cis = CodedInputStream.newInstance(in)
    cis.readInt32() match {
      case v if v >= 5 && v <= (NonEmptySeqVersion) =>
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary, v)
        val seq: NonEmptySeq[SoQLAnalysis[C, T]] = deserializer.read()
        val bt: BinaryTree[SoQLAnalysis[C, T]] = toBinaryTree(seq.seq)
        bt
      case v if v > NonEmptySeqVersion =>
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary, v)
        val bt: BinaryTree[SoQLAnalysis[C, T]] = deserializer.readBinaryTree(deserializer.readAnalysis())
        bt
      case v if v == TestVersionV5 => // single select
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary, v)
        val seq = NonEmptySeq(deserializer.readAnalysis(), Seq.empty)
        val bt: BinaryTree[SoQLAnalysis[C, T]] = toBinaryTree(seq.seq)
        bt
      case other =>
        throw new UnknownAnalysisSerializationVersion(other)
    }
  }

  // Not designed for empty seq
  def toBinaryTree(seq: Seq[SoQLAnalysis[C, T]]): BinaryTree[SoQLAnalysis[C, T]] = {
    def buildBinaryTree(seq: Seq[SoQLAnalysis[C, T]]): BinaryTree[SoQLAnalysis[C, T]] = {
      seq match {
        case Seq(x) => Leaf(x)
        case ss =>
          PipeQuery(buildBinaryTree(ss.dropRight(1)), Leaf(ss.last))
      }
    }

    buildBinaryTree(seq)
  }
}
