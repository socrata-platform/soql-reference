package com.socrata.soql

import scala.util.parsing.input.{NoPosition, Position}

import java.io.{ByteArrayOutputStream, OutputStream}

import com.google.protobuf.CodedOutputStream
import gnu.trove.impl.Constants
import gnu.trove.map.hash.TObjectIntHashMap

import com.socrata.soql.environment.ColumnName
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.typed._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.collection.OrderedMap

private trait SerializationDictionary[T] {
  def registerType(typ: T): Int
  def registerString(s: String): Int
  def registerColumn(col: ColumnName): Int
  def registerFunction(func: MonomorphicFunction[T]): Int
}

class AnalysisSerializer[T](serializeType: (CodedOutputStream, T) => Unit) extends ((OutputStream, SoQLAnalysis[T]) => Unit) {
  private class SerializationDictionaryImpl extends SerializationDictionary[T] {
    private def makeMap[A] = new TObjectIntHashMap[A](Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1)
    private def register[A](map: TObjectIntHashMap[A], a: A): Int = {
      val count = map.size()
      map.putIfAbsent(a, count) match {
        case -1 => count
        case old => old
      }
    }

    val types = makeMap[T] // This MUST be written BEFORE functionsUnsafe!
    val strings = makeMap[String] // This MUST be written BEFORE columns and functions!
    val columns = makeMap[ColumnName]
    val functions = makeMap[MonomorphicFunction[T]]

    def registerType(typ: T): Int =
      register(types, typ)

    def registerString(s: String): Int =
      register(strings, s)

    def registerColumn(col: ColumnName): Int = {
      val count = columns.size
      columns.putIfAbsent(col, count) match {
        case -1 =>
          registerString(col.name)
          count
        case id =>
          id
      }
    }

    def registerFunction(func: MonomorphicFunction[T]): Int = {
      val count = functions.size
      functions.putIfAbsent(func, count) match {
        case -1 =>
          registerString(func.function.identity)
          func.bindings.foreach { case (typeVar, typ) =>
            registerString(typeVar)
            registerType(typ)
          }
          count
        case id =>
          id
      }
    }

    private def saveRegistry[A](out: CodedOutputStream, registry: TObjectIntHashMap[A])(f: A => Unit) {
      out.writeUInt32NoTag(registry.size)
      val it = registry.iterator
      while(it.hasNext) {
        it.advance()
        f(it.key)
        out.writeUInt32NoTag(it.value)
      }
    }

    private def saveTypes(out: CodedOutputStream) =
      saveRegistry(out, types) { typ =>
        serializeType(out, typ)
      }

    private def saveColumns(out: CodedOutputStream) =
      saveRegistry(out: CodedOutputStream, columns) { col =>
        out.writeUInt32NoTag(strings.get(col.name))
      }

    private def saveFunctions(out: CodedOutputStream) =
      saveRegistry(out, functions) { case MonomorphicFunction(function, bindings) =>
        out.writeUInt32NoTag(strings.get(function.identity))
        out.writeUInt32NoTag(bindings.size)
        for((typeVar, typ) <- bindings) {
          out.writeUInt32NoTag(strings.get(typeVar))
          out.writeUInt32NoTag(types.get(typ))
        }
      }

    private def saveStrings(out: CodedOutputStream) =
      saveRegistry(out, strings) { s =>
        out.writeStringNoTag(s)
      }

    def save(out: CodedOutputStream) {
      saveTypes(out)
      saveStrings(out)
      saveColumns(out)
      saveFunctions(out)
    }
  }

  private class Serializer(out: CodedOutputStream, dictionary: SerializationDictionary[T]) {
    import dictionary._

    private def writePosition(pos: Position) {
      pos match {
        case SoQLPosition(line, column, sourceText, offset) =>
          out.writeRawByte(0)
          out.writeUInt32NoTag(line)
          out.writeUInt32NoTag(column)
          out.writeUInt32NoTag(dictionary.registerString(sourceText))
          out.writeUInt32NoTag(offset)
        case NoPosition =>
          out.writeRawByte(1)
        case other =>
          out.writeRawByte(2)
          out.writeUInt32NoTag(pos.line)
          out.writeUInt32NoTag(pos.column)
          // the position doesn't expose the raw line value *sigh*...
          // We'll just have to hope longString hasn't been overridden.
          val ls = pos.longString
          val newlinePos = ls.indexOf('\n')
          val line = if(newlinePos == -1) "" else ls.substring(0, newlinePos)
          out.writeUInt32NoTag(dictionary.registerString(line))
      }
    }

    private def writeExpr(e: CoreExpr[T]) {
      writePosition(e.position)
      e match {
        case ColumnRef(col, typ) =>
          out.writeRawByte(1)
          out.writeUInt32NoTag(registerColumn(col))
          out.writeUInt32NoTag(registerType(typ))
        case StringLiteral(value, typ) =>
          out.writeRawByte(2)
          out.writeUInt32NoTag(registerString(value))
          out.writeUInt32NoTag(registerType(typ))
        case NumberLiteral(value, typ) =>
          out.writeRawByte(3)
          out.writeUInt32NoTag(registerString(value.toString))
          out.writeUInt32NoTag(registerType(typ))
        case BooleanLiteral(value, typ) =>
          out.writeRawByte(4)
          out.writeBoolNoTag(value)
          out.writeUInt32NoTag(registerType(typ))
        case NullLiteral(typ) =>
          out.writeRawByte(5)
          out.writeUInt32NoTag(registerType(typ))
        case f@FunctionCall(func, params) =>
          out.writeRawByte(6)
          writePosition(f.functionNamePosition)
          out.writeUInt32NoTag(registerFunction(func))
          out.writeUInt32NoTag(params.length)
          for(param <- params) writeExpr(param)
      }
    }

    private def writeGrouped(isGrouped: Boolean) =
      out.writeBoolNoTag(isGrouped)

    private def writeSelection(selection: OrderedMap[ColumnName, CoreExpr[T]]) {
      out.writeUInt32NoTag(selection.size)
      for((col, expr) <- selection) {
        out.writeUInt32NoTag(dictionary.registerColumn(col))
        writeExpr(expr)
      }
    }

    private def maybeWrite[A](x: Option[A])(f: A => Unit): Unit = x match {
      case Some(a) =>
        out.writeBoolNoTag(true)
        f(a)
      case None =>
        out.writeBoolNoTag(false)
    }

    private def writeWhere(where: Option[CoreExpr[T]]) =
      maybeWrite(where) { expr =>
        writeExpr(expr)
      }

    private def writeGroupBy(groupBy: Option[Seq[CoreExpr[T]]]) =
      maybeWrite(groupBy) { exprs =>
        out.writeUInt32NoTag(exprs.size)
        exprs.foreach(writeExpr)
      }

    private def writeHaving(expr: Option[CoreExpr[T]]) =
      writeWhere(expr)

    private def writeSingleOrderBy(orderBy: OrderBy[T]) = {
      val OrderBy(expr, ascending, nullsLast) = orderBy
      writeExpr(expr)
      out.writeBoolNoTag(ascending)
      out.writeBoolNoTag(nullsLast)
    }

    private def writeOrderBy(orderBy: Option[Seq[OrderBy[T]]]) =
      maybeWrite(orderBy) { orderBys =>
        out.writeUInt32NoTag(orderBys.size)
        orderBys.foreach(writeSingleOrderBy)
      }

    private def writeLimit(limit: Option[BigInt]) =
      maybeWrite(limit) { n =>
        out.writeStringNoTag(n.toString)
      }

    private def writeOffset(offset: Option[BigInt]) =
      writeLimit(offset)

    private def writeSearch(search: Option[String]) =
      maybeWrite(search) { s =>
        out.writeStringNoTag(s)
      }

    def write(analysis: SoQLAnalysis[T]) {
      val SoQLAnalysis(isGrouped,
                       selection,
                       where,
                       groupBy,
                       having,
                       orderBy,
                       limit,
                       offset,
                       search) = analysis
      writeGrouped(isGrouped)
      writeSelection(selection)
      writeWhere(where)
      writeGroupBy(groupBy)
      writeHaving(having)
      writeOrderBy(orderBy)
      writeLimit(limit)
      writeOffset(offset)
      writeSearch(search)
    }
  }

  def apply(outputStream: OutputStream, analysis: SoQLAnalysis[T]) {
    val dictionary = new SerializationDictionaryImpl
    val postDictionaryData = new ByteArrayOutputStream
    val out = CodedOutputStream.newInstance(postDictionaryData)
    val serializer = new Serializer(out, dictionary)
    serializer.write(analysis)
    out.flush()

    val codedOutputStream = CodedOutputStream.newInstance(outputStream)
    codedOutputStream.writeRawByte(0) // version number
    dictionary.save(codedOutputStream)
    codedOutputStream.flush()
    postDictionaryData.writeTo(outputStream)
  }
}
