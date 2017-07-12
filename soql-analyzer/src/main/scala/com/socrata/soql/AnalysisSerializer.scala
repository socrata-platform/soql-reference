package com.socrata.soql

import scala.util.parsing.input.{NoPosition, Position}
import java.io.{ByteArrayOutputStream, OutputStream}

import com.google.protobuf.CodedOutputStream
import gnu.trove.impl.Constants
import gnu.trove.map.hash.TObjectIntHashMap
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.typed._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}

private trait SerializationDictionary[C,T] {
  def registerType(typ: T): Int
  def registerString(s: String): Int
  def registerColumn(col: C): Int
  def registerLabel(name: ColumnName): Int
  def registerFunction(func: MonomorphicFunction[T]): Int
}

trait UnchainedAnalysisSerializerProvider[C, T] {
  def unchainedSerializer: (OutputStream, SoQLAnalysis[C, T]) => Unit
}

class AnalysisSerializer[C,T](serializeColumn: C => String, serializeType: T => String) extends ((OutputStream, Seq[SoQLAnalysis[C, T]]) => Unit) with UnchainedAnalysisSerializerProvider[C, T] {
  type Expr = CoreExpr[C, T]
  type Order = OrderBy[C, T]

  private class SerializationDictionaryImpl extends SerializationDictionary[C, T] {
    private def makeMap[A] = new TObjectIntHashMap[A](Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1)
    private def register[A](map: TObjectIntHashMap[A], a: A): Int = {
      val count = map.size()
      map.putIfAbsent(a, count) match {
        case -1 => count
        case old => old
      }
    }

    val strings = makeMap[String] // This MUST be written BEFORE types, labels, columns and functions!
    val types = makeMap[T] // This MUST be written BEFORE functionsUnsafe!
    val labels = makeMap[ColumnName]
    val columns = makeMap[C]
    val functions = makeMap[MonomorphicFunction[T]]

    def registerString(s: String): Int =
      register(strings, s)

    def registerType(typ: T): Int =
      types.get(typ) match {
        case -1 =>
          val id = registerString(serializeType(typ))
          types.put(typ, id)
          id
        case id =>
          id
      }

    def registerColumn(col: C): Int =
      columns.get(col) match {
        case -1 =>
          val id = registerString(serializeColumn(col))
          columns.put(col, id)
          id
        case id =>
          id
      }

    def registerLabel(col: ColumnName): Int = {
      val size = labels.size()
      labels.putIfAbsent(col, size) match {
        case -1 =>
          registerString(col.name)
          labels.put(col, size)
          size
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
        out.writeUInt32NoTag(types.get(typ))
      }

    private def saveColumns(out: CodedOutputStream) =
      saveRegistry(out: CodedOutputStream, columns) { col =>
        out.writeUInt32NoTag(columns.get(col))
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

    private def saveLabels(out: CodedOutputStream) =
      saveRegistry(out, labels) { l =>
        out.writeUInt32NoTag(strings.get(l.name))
      }

    def save(out: CodedOutputStream) {
      saveStrings(out)
      saveLabels(out)
      saveTypes(out)
      saveColumns(out)
      saveFunctions(out)
    }
  }

  private class Serializer(out: CodedOutputStream, dictionary: SerializationDictionary[C, T]) {
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

    private def writeExpr(e: Expr) {
      writePosition(e.position)
      e match {
        case ColumnRef(qual, col, typ) =>
          out.writeRawByte(1)
          maybeWrite(qual) { x => out.writeStringNoTag(x) }
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
        case LongLiteral(value, typ) =>
          out.writeRawByte(7)
          out.writeUInt32NoTag(registerString(value.toString))
          out.writeUInt32NoTag(registerType(typ))
      }
    }

    private def writeGrouped(isGrouped: Boolean) =
      out.writeBoolNoTag(isGrouped)

    private def writeDistinct(distinct: Boolean) =
      out.writeBoolNoTag(distinct)

    private def writeSelection(selection: OrderedMap[ColumnName, Expr]) {
      out.writeUInt32NoTag(selection.size)
      for((col, expr) <- selection) {
        out.writeUInt32NoTag(dictionary.registerLabel(col))
        writeExpr(expr)
      }
    }

    private def writeJoins(joins: Option[List[Join[C, T]]]) {
      val size = joins.map(_.size).getOrElse(0)
      out.writeUInt32NoTag(size)
      joins.toList.flatten.foreach { join =>
        out.writeStringNoTag(join.typ.toString)
        write(join.tableLike)
        maybeWrite(join.alias)(x => out.writeStringNoTag(x))
        writeExpr(join.expr)
      }
    }

    private def maybeWrite[A](x: Option[A])(f: A => Unit): Unit = x match {
      case Some(a) =>
        out.writeBoolNoTag(true)
        f(a)
      case None =>
        out.writeBoolNoTag(false)
    }

    private def writeWhere(where: Option[Expr]) =
      maybeWrite(where) { expr =>
        writeExpr(expr)
      }

    private def writeGroupBy(groupBy: Option[Seq[Expr]]) =
      maybeWrite(groupBy) { exprs =>
        out.writeUInt32NoTag(exprs.size)
        exprs.foreach(writeExpr)
      }

    private def writeHaving(expr: Option[Expr]) =
      writeWhere(expr)

    private def writeSingleOrderBy(orderBy: Order) = {
      val OrderBy(expr, ascending, nullsLast) = orderBy
      writeExpr(expr)
      out.writeBoolNoTag(ascending)
      out.writeBoolNoTag(nullsLast)
    }

    private def writeOrderBy(orderBy: Option[Seq[Order]]) =
      maybeWrite(orderBy) { orderBys =>
        out.writeUInt32NoTag(orderBys.size)
        orderBys.foreach(writeSingleOrderBy)
      }

    private def writeLimit(limit: Option[BigInt]) =
      maybeWrite(limit) { n =>
        out.writeUInt32NoTag(registerString(n.toString))
      }

    private def writeOffset(offset: Option[BigInt]) =
      writeLimit(offset)

    private def writeSearch(search: Option[String]) =
      maybeWrite(search) { s =>
        out.writeUInt32NoTag(registerString(s))
      }

    def writeAnalysis(analysis: SoQLAnalysis[C, T]) {
      val SoQLAnalysis(isGrouped,
                       distinct,
                       selection,
                       from,
                       join,
                       where,
                       groupBy,
                       having,
                       orderBy,
                       limit,
                       offset,
                       search) = analysis
      writeGrouped(isGrouped)
      writeDistinct(analysis.distinct)
      writeSelection(selection)
      maybeWrite(from) { x => out.writeStringNoTag(x) }
      writeJoins(join)
      writeWhere(where)
      writeGroupBy(groupBy)
      writeHaving(having)
      writeOrderBy(orderBy)
      writeLimit(limit)
      writeOffset(offset)
      writeSearch(search)
    }

    def write(analyses: Seq[SoQLAnalysis[C, T]]): Unit = {
      out.writeInt32NoTag(analyses.length)
      analyses.foreach(writeAnalysis)
    }
  }

  def apply(outputStream: OutputStream, analyses: Seq[SoQLAnalysis[C, T]]) {
    val dictionary = new SerializationDictionaryImpl
    val postDictionaryData = new ByteArrayOutputStream
    val out = CodedOutputStream.newInstance(postDictionaryData)
    val serializer = new Serializer(out, dictionary)
    serializer.write(analyses)
    out.flush()

    val codedOutputStream = CodedOutputStream.newInstance(outputStream)
    codedOutputStream.writeInt32NoTag(3) // version number
    dictionary.save(codedOutputStream)
    codedOutputStream.flush()
    postDictionaryData.writeTo(outputStream)
  }

  // For migration: exposes a serializer that will serialize a singleton
  // analysis.  There doesn't need to be a special deserializer for it;
  // the chained deserializer understands this format.
  //
  // This is different from apply(out, Seq(a)) in that this generates
  // a version 0 serialization but that generates a version 1.  It lets
  // us decouple (a bit) the upgrade paths of the various parts which
  // use this library.
  def unchainedSerializer: (OutputStream, SoQLAnalysis[C, T]) => Unit = { (outputStream, analyses) =>
    val dictionary = new SerializationDictionaryImpl
    val postDictionaryData = new ByteArrayOutputStream
    val out = CodedOutputStream.newInstance(postDictionaryData)
    val serializer = new Serializer(out, dictionary)
    serializer.writeAnalysis(analyses)
    out.flush()

    val codedOutputStream = CodedOutputStream.newInstance(outputStream)
    codedOutputStream.writeInt32NoTag(0) // version number
    dictionary.save(codedOutputStream)
    codedOutputStream.flush()
    postDictionaryData.writeTo(outputStream)
  }
}
