package com.socrata.soql

import java.io.InputStream

import com.google.protobuf.CodedInputStream
import com.socrata.soql.ast.{JoinType, InnerJoinType, LeftOuterJoinType, RightOuterJoinType, FullOuterJoinType}
import com.socrata.soql.collection.{OrderedMap, NonEmptySeq}
import com.socrata.soql.environment.{ColumnName, ResourceName, Qualified, TableRef}
import com.socrata.soql.functions.{Function, MonomorphicFunction}
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.typed._
import gnu.trove.map.hash.TIntObjectHashMap

import scala.util.parsing.input.{NoPosition, Position}

private case class SimplePosition(line: Int, column: Int, lineContents: String) extends Position

class UnknownAnalysisSerializationVersion(val version: Int) extends Exception("Unknown analysis serialization version " + version)

private trait DeserializationDictionary[T] {
  def types(i: Int): T
  def resourceNames(i: Int): ResourceName
  def columnNames(i: Int): ColumnName
  def functions(i: Int): MonomorphicFunction[T]
  def strings(i: Int): String
}

object AnalysisDeserializer {
  val TestVersionV10 = 10
  val CurrentVersion = TestVersionV10
}

class AnalysisDeserializer[T](typeDeserializer: String => T, functionMap: String => Function[T]) extends (InputStream => NonEmptySeq[SoQLAnalysis[Qualified[ColumnName], T]]) {
  import AnalysisDeserializer._

  type Expr = CoreExpr[Qualified[ColumnName], T]
  type Order = OrderBy[Qualified[ColumnName], T]

  private class DeserializationDictionaryImpl(typesRegistry: TIntObjectHashMap[T],
                                              stringsRegistry: TIntObjectHashMap[String],
                                              resourceNamesRegistry: TIntObjectHashMap[ResourceName],
                                              columnsRegistry: TIntObjectHashMap[ColumnName],
                                              functionsRegistry: TIntObjectHashMap[MonomorphicFunction[T]])
    extends DeserializationDictionary[T]
  {
    def types(i: Int): T = typesRegistry.get(i)
    def strings(i: Int): String = stringsRegistry.get(i)
    def resourceNames(i: Int): ResourceName = resourceNamesRegistry.get(i)
    def columnNames(i: Int): ColumnName = columnsRegistry.get(i)
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

    private def readSimpleRegistry[A](in: CodedInputStream, strings: TIntObjectHashMap[String], f: String => A): TIntObjectHashMap[A] = {
      val result = new TIntObjectHashMap[A]
      (1 to in.readUInt32()).foreach { _ =>
        val i = in.readUInt32()
        result.put(i, f(strings.get(i)))
      }
      result
    }

    def fromInput(in: CodedInputStream): DeserializationDictionary[T] = {
      val strings = readRegistry(in) {
        in.readString()
      }
      val resourceNames = readSimpleRegistry(in, strings, ResourceName)
      val columnNames = readSimpleRegistry(in, strings, ColumnName)
      val types = readSimpleRegistry(in, strings, typeDeserializer)
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

      new DeserializationDictionaryImpl(types, strings, resourceNames, columnNames, functions)
    }
  }

  private class Deserializer(in: CodedInputStream,
                             dictionary: DeserializationDictionary[T],
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
          val name = readQualified()
          val typ = dictionary.types(in.readUInt32())
          ColumnRef(name, typ)(pos)
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
        val name =  dictionary.columnNames(in.readUInt32())
        val expr = readExpr()
        name -> expr
      }
      OrderedMap(elems: _*)
    }

    def readJoins(): Seq[Join[Qualified[ColumnName], T]] = {
      readSeq {
        val joinType = readJoinType()
        val joinAnalysis = readJoinAnalysis()
        Join(joinType, joinAnalysis, readExpr())
      }
    }

    def readJoinType() =
      in.readRawByte() match {
        case 1 => InnerJoinType
        case 2 => LeftOuterJoinType
        case 3 => RightOuterJoinType
        case 4 => FullOuterJoinType
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

    def readQualified(): Qualified[ColumnName] = {
      Qualified(readTableRef(),
                          dictionary.columnNames(in.readUInt32()))
    }

    def readTableRef(): TableRef =
      in.readRawByte() match {
        case 1 =>
          TableRef.Primary(dictionary.resourceNames(in.readUInt32()))
        case 2 =>
          TableRef.PreviousChainStep
        case 3 =>
          TableRef.Join(in.readUInt32())
      }

    def readJoinAnalysis(): JoinAnalysis[Qualified[ColumnName], T] = {
      JoinAnalysis(TableRef.Primary(dictionary.resourceNames(in.readUInt32())),
                   readSeq { readAnalysis() },
                   TableRef.Join(in.readUInt32()))
    }

    def readSearch(): Option[String] =
      maybeRead {
        dictionary.strings(in.readUInt32())
      }

    def readAnalysis(): SoQLAnalysis[Qualified[ColumnName], T] = {
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

      SoQLAnalysis(ig, d, s, j, w, gb, h, ob, l, o, search)
    }

    def read(): NonEmptySeq[SoQLAnalysis[Qualified[ColumnName], T]] = {
      readNonEmptySeq { readAnalysis() }
    }
  }

  def apply(in: InputStream): NonEmptySeq[SoQLAnalysis[Qualified[ColumnName], T]] = {
    val cis = CodedInputStream.newInstance(in)
    cis.readInt32() match {
      case v@CurrentVersion =>
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary, v)
        deserializer.read()
      case other =>
        throw new UnknownAnalysisSerializationVersion(other)
    }
  }
}
