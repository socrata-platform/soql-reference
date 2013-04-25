package com.socrata.soql

import scala.util.parsing.input.{NoPosition, Position}
import scala.collection.immutable.VectorBuilder

import java.io.InputStream

import com.google.protobuf.CodedInputStream
import gnu.trove.map.hash.TIntObjectHashMap

import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.{MonomorphicFunction, Function}
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.typed._
import com.socrata.soql.collection.OrderedMap

private case class SimplePosition(line: Int, column: Int, lineContents: String) extends Position

class UnknownAnalysisSerializationVersion(val version: Int) extends Exception("Unknown analysis serialization version " + version)

private trait DeserializationDictionary[T] {
  def types(i: Int): T
  def columns(i: Int): ColumnName
  def functions(i: Int): MonomorphicFunction[T]
  def strings(i: Int): String
}

class AnalysisDeserializer[T](typeDeserializer: CodedInputStream => T, functionMap: String => Function[T]) extends (InputStream => SoQLAnalysis[T]) {
  private class DeserializationDictionaryImpl(typesRegistry: TIntObjectHashMap[T],
                                      stringsRegistry: TIntObjectHashMap[String],
                                      columnsRegistry: TIntObjectHashMap[ColumnName],
                                      functionsRegistry: TIntObjectHashMap[MonomorphicFunction[T]])
    extends DeserializationDictionary[T]
  {
    def types(i: Int): T = typesRegistry.get(i)
    def strings(i: Int): String = stringsRegistry.get(i)
    def columns(i: Int): ColumnName = columnsRegistry.get(i)
    def functions(i: Int): MonomorphicFunction[T] = functionsRegistry.get(i)
  }

  private object DeserializationDictionaryImpl {
    private def readRegistry[A](in: CodedInputStream)(f: => A): TIntObjectHashMap[A] = {
      val result = new TIntObjectHashMap[A]
      for(_ <- 1 to in.readUInt32()) {
        val a = f
        val i = in.readUInt32()
        result.put(i, a)
      }
      result
    }

    def fromInput(in: CodedInputStream): DeserializationDictionary[T] = {
      val types = readRegistry(in) {
        typeDeserializer(in)
      }
      val strings = readRegistry(in) {
        in.readString()
      }
      val columns = readRegistry(in) {
        ColumnName(strings.get(in.readUInt32()))
      }
      val functions = readRegistry(in) {
        val function = functionMap(strings.get(in.readUInt32()))
        val bindingsBuilder = Map.newBuilder[String, T]
        for(_ <- 1 to in.readUInt32()) {
          val typeVar = strings.get(in.readUInt32())
          val typ = types.get(in.readUInt32())
          bindingsBuilder += typeVar -> typ
        }
        MonomorphicFunction(function, bindingsBuilder.result())
      }

      new DeserializationDictionaryImpl(types, strings, columns, functions)
    }
  }

  private class Deserializer(in: CodedInputStream,
                             dictionary: DeserializationDictionary[T])
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

    def readExpr(): CoreExpr[T] = {
      val pos = readPosition()
      in.readRawByte() match {
        case 1 =>
          val name = dictionary.columns(in.readUInt32())
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
          val params = (1 to in.readUInt32()).map { _ =>
            readExpr()
          }
          FunctionCall(func, params)(pos, functionNamePosition)
      }
    }

    def readIsGrouped(): Boolean =
      in.readBool()

    def maybeRead[A](f: => A): Option[A] =
      if(in.readBool()) Some(f)
      else None

    def readSelection(): OrderedMap[ColumnName, CoreExpr[T]] = {
      val count = in.readUInt32()
      val elems = new VectorBuilder[(ColumnName, CoreExpr[T])]
      for(_ <- 1 to count) {
        val name = dictionary.columns(in.readUInt32())
        val expr = readExpr()
        elems += name -> expr
      }
      OrderedMap(elems.result() : _*)
    }

    def readWhere(): Option[CoreExpr[T]] =
      maybeRead {
        readExpr()
      }

    def readGroupBy(): Option[Seq[CoreExpr[T]]] =
      maybeRead {
        val count = in.readUInt32()
        (1 to count) map { _ =>
          readExpr()
        }
      }

    def readHaving() = readWhere()

    def readOrderBy(): Option[Seq[OrderBy[T]]] =
      maybeRead {
        val count = in.readUInt32()
        (1 to count) map { _ =>
          val expr = readExpr()
          val ascending = in.readBool()
          val nullsLast = in.readBool()
          OrderBy(expr, ascending, nullsLast)
        }
      }

    def readLimit(): Option[BigInt] =
      maybeRead {
        BigInt(in.readString())
      }

    def readOffset() = readLimit()

    def readSearch(): Option[String] =
      maybeRead {
        in.readString()
      }


    def read(): SoQLAnalysis[T] = {
      val isGrouped = readIsGrouped()
      val selection = readSelection()
      val where = readWhere()
      val groupBy = readGroupBy()
      val having = readHaving()
      val orderBy = readOrderBy()
      val limit = readLimit()
      val offset = readOffset()
      val search = readSearch()
      SoQLAnalysis(
        isGrouped,
        selection,
        where,
        groupBy,
        having,
        orderBy,
        limit,
        offset,
        search)
    }
  }

  def apply(in: InputStream): SoQLAnalysis[T] = {
    val cis = CodedInputStream.newInstance(in)
    cis.readRawByte() match {
      case 0 =>
        val dictionary = DeserializationDictionaryImpl.fromInput(cis)
        val deserializer = new Deserializer(cis, dictionary)
        deserializer.read()
      case other =>
        throw new UnknownAnalysisSerializationVersion(other)
    }
  }
}
