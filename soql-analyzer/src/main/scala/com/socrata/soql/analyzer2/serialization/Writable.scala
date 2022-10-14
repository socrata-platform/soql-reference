package com.socrata.soql.analyzer2.serialization

import scala.util.parsing.input.{Position, NoPosition}

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ResourceName, TypeName, ColumnName}
import com.socrata.soql.parsing.SoQLPosition

trait Writable[T] {
  def writeTo(buffer: WriteBuffer, t: T): Unit
}
object Writable {
  implicit object boolean extends Writable[Boolean] {
    def writeTo(buffer: WriteBuffer, b: Boolean): Unit =
      buffer.data.writeBoolNoTag(b)
  }

  implicit object int extends Writable[Int] {
    def writeTo(buffer: WriteBuffer, i: Int): Unit =
      buffer.data.writeSInt32NoTag(i)
  }

  implicit object long extends Writable[Long] {
    def writeTo(buffer: WriteBuffer, i: Long): Unit =
      buffer.data.writeSInt64NoTag(i)
  }

  implicit object bigint extends Writable[BigInt] {
    def writeTo(buffer: WriteBuffer, i: BigInt): Unit =
      string.writeTo(buffer, i.toString)
  }

  implicit object string extends Writable[String] {
    def writeTo(buffer: WriteBuffer, s: String): Unit = {
      buffer.data.writeUInt32NoTag(buffer.strings.add(s))
    }
  }

  implicit object resourceName extends Writable[ResourceName] {
    def writeTo(buffer: WriteBuffer, rn: ResourceName): Unit =
      string.writeTo(buffer, rn.name)
  }

  implicit object typeName extends Writable[TypeName] {
    def writeTo(buffer: WriteBuffer, tn: TypeName): Unit =
      string.writeTo(buffer, tn.name)
  }

  implicit object columnName extends Writable[ColumnName] {
    def writeTo(buffer: WriteBuffer, tn: ColumnName): Unit =
      string.writeTo(buffer, tn.name)
  }

  implicit object position extends Writable[Position] {
    def writeTo(buffer: WriteBuffer, pos: Position): Unit = {
      pos match {
        case SoQLPosition(line, column, sourceText, offset) =>
          buffer.data.writeRawByte(0)
          buffer.data.writeUInt32NoTag(line)
          buffer.data.writeUInt32NoTag(column)
          buffer.write(sourceText)
          buffer.data.writeUInt32NoTag(offset)
        case NoPosition =>
          buffer.data.writeRawByte(1)
        case other =>
          buffer.data.writeRawByte(2)
          buffer.data.writeUInt32NoTag(pos.line)
          buffer.data.writeUInt32NoTag(pos.column)
          // the position doesn't expose the raw line value *sigh*...
          // We'll just have to hope longString hasn't been overridden.
          val ls = pos.longString
          val newlinePos = ls.indexOf('\n')
          val line = if(newlinePos == -1) "" else ls.substring(0, newlinePos)
          buffer.write(line)
      }
    }
  }

  implicit def seq[T : Writable] = new Writable[Seq[T]] {
    def writeTo(buffer: WriteBuffer, ts: Seq[T]): Unit = {
      buffer.data.writeUInt32NoTag(ts.length)
      for(t <- ts) {
        buffer.write(t)
      }
    }
  }

  implicit def nonEmptySeq[T : Writable] = new Writable[NonEmptySeq[T]] {
    def writeTo(buffer: WriteBuffer, ts: NonEmptySeq[T]): Unit = {
      buffer.write(ts.head)
      buffer.write(ts.tail)
    }
  }

  implicit def set[T : Writable] = new Writable[Set[T]] {
    def writeTo(buffer: WriteBuffer, ts: Set[T]): Unit = {
      buffer.data.writeUInt32NoTag(ts.size)
      for(t <- ts) {
        buffer.write(t)
      }
    }
  }

  implicit def map[T : Writable, U: Writable] = new Writable[Map[T, U]] {
    def writeTo(buffer: WriteBuffer, m: Map[T, U]): Unit = {
      buffer.data.writeUInt32NoTag(m.size)
      for((k, v) <- m) {
        buffer.write(k)
        buffer.write(v)
      }
    }
  }

  implicit def orderdMap[T : Writable, U: Writable] = new Writable[OrderedMap[T, U]] {
    def writeTo(buffer: WriteBuffer, m: OrderedMap[T, U]): Unit = {
      buffer.data.writeUInt32NoTag(m.size)
      for((k, v) <- m) {
        buffer.write(k)
        buffer.write(v)
      }
    }
  }

  implicit def option[T : Writable] = new Writable[Option[T]] {
    def writeTo(buffer: WriteBuffer, m: Option[T]): Unit = {
      m match {
        case None =>
          buffer.data.writeUInt32NoTag(0)
        case Some(v) =>
          buffer.data.writeUInt32NoTag(1)
          buffer.write(v)
      }
    }
  }

  implicit def pair[T: Writable, U: Writable] = new Writable[(T, U)] {
    def writeTo(buffer: WriteBuffer, pair: (T, U)): Unit = {
      buffer.write(pair._1)
      buffer.write(pair._2)
    }
  }
}
