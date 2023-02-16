package com.socrata.soql.serialize

import scala.util.parsing.input.{Position, NoPosition}

import java.io.{IOException, InputStream}

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ResourceName, TypeName, ColumnName}
import com.socrata.soql.parsing.SoQLPosition

trait Readable[T] {
  def readFrom(buffer: ReadBuffer): T

  protected def fail(msg: String): Nothing = throw new IOException(msg)
}

object Readable extends `-impl`.ReadableTuples {
  private case class SimplePosition(line: Int, column: Int, lineContents: String) extends Position

  implicit object unit extends Readable[Unit] {
    def readFrom(buffer: ReadBuffer): Unit = {
    }
  }

  implicit object bool extends Readable[Boolean] {
    def readFrom(buffer: ReadBuffer): Boolean = {
      buffer.data.readBool()
    }
  }

  implicit object int extends Readable[Int] {
    def readFrom(buffer: ReadBuffer): Int = {
      buffer.data.readSInt32()
    }
  }

  implicit object long extends Readable[Long] {
    def readFrom(buffer: ReadBuffer): Long = {
      buffer.data.readSInt64()
    }
  }

  implicit object bigint extends Readable[BigInt] {
    def readFrom(buffer: ReadBuffer): BigInt = {
      try {
        BigInt(string.readFrom(buffer))
      } catch {
        case e: NumberFormatException =>
          fail("Invalid serialized BigInt")
      }
    }
  }

  implicit object string extends Readable[String] {
    def readFrom(buffer: ReadBuffer): String = {
      buffer.strings(buffer.data.readUInt32())
    }
  }

  implicit object resourceName extends Readable[ResourceName] {
    def readFrom(buffer: ReadBuffer): ResourceName = {
      ResourceName(string.readFrom(buffer))
    }
  }

  implicit object typeName extends Readable[TypeName] {
    def readFrom(buffer: ReadBuffer): TypeName = {
      TypeName(string.readFrom(buffer))
    }
  }

  implicit object columnName extends Readable[ColumnName] {
    def readFrom(buffer: ReadBuffer): ColumnName = {
      ColumnName(string.readFrom(buffer))
    }
  }

  implicit object position extends Readable[Position] {
    def readFrom(buffer: ReadBuffer): Position = {
      buffer.data.readRawByte() match {
        case 0 =>
          val line = buffer.data.readUInt32()
          val column = buffer.data.readUInt32()
          val source = buffer.read[String]()
          val offset = buffer.data.readUInt32()
          SoQLPosition(line, column, source, offset)
        case 1 =>
          NoPosition
        case 2 =>
          val line = buffer.data.readUInt32()
          val column = buffer.data.readUInt32()
          val lineText = buffer.read[String]()
          SimplePosition(line, column, lineText)
        case other =>
          fail("Unknown position type " + other)
      }
    }
  }

  implicit def bytes = new Readable[Array[Byte]] {
    def readFrom(buffer: ReadBuffer): Array[Byte] = {
      var n = buffer.data.readUInt32()
      buffer.data.readRawBytes(n)
    }
  }

  implicit def seq[T : Readable] = new Readable[Seq[T]] {
    def readFrom(buffer: ReadBuffer): Seq[T] = {
      var n = buffer.data.readUInt32()
      val vb = Vector.newBuilder[T]
      while(n != 0) {
        n -= 1
        vb += buffer.read[T]()
      }
      vb.result()
    }
  }

  implicit def nonEmptySeq[T : Readable] = new Readable[NonEmptySeq[T]] {
    def readFrom(buffer: ReadBuffer): NonEmptySeq[T] = {
      var n = buffer.data.readUInt32()
      if(n == 0) fail("Expected non-empty seq")
      val head = buffer.read[T]()
      val vb = Vector.newBuilder[T]
      while(n != 1) {
        n -= 1
        vb += buffer.read[T]()
      }
      NonEmptySeq(head, vb.result())
    }
  }

  implicit def set[T : Readable] = new Readable[Set[T]] {
    def readFrom(buffer: ReadBuffer): Set[T] = {
      var n = buffer.data.readUInt32()
      val sb = Set.newBuilder[T]
      while(n != 0) {
        n -= 1
        sb += buffer.read[T]()
      }
      sb.result()
    }
  }

  implicit def map[T : Readable, U : Readable] = new Readable[Map[T, U]] {
    def readFrom(buffer: ReadBuffer): Map[T, U] = {
      var n = buffer.data.readUInt32()
      val mb = Map.newBuilder[T, U]
      while(n != 0) {
        n -= 1
        mb += buffer.read[T]() -> buffer.read[U]()
      }
      mb.result()
    }
  }

  implicit def orderedMap[T : Readable, U : Readable] = new Readable[OrderedMap[T, U]] {
    def readFrom(buffer: ReadBuffer): OrderedMap[T, U] = {
      var n = buffer.data.readUInt32()
      val mb = Vector.newBuilder[(T, U)]
      while(n != 0) {
        n -= 1
        mb += buffer.read[T]() -> buffer.read[U]()
      }
      OrderedMap() ++ mb.result()
    }
  }

  implicit def orderedSet[T : Readable] = new Readable[OrderedSet[T]] {
    def readFrom(buffer: ReadBuffer): OrderedSet[T] = {
      var n = buffer.data.readUInt32()
      val mb = Vector.newBuilder[T]
      while(n != 0) {
        n -= 1
        mb += buffer.read[T]()
      }
      OrderedSet() ++ mb.result()
    }
  }

  implicit def option[T : Readable] = new Readable[Option[T]] {
    def readFrom(buffer: ReadBuffer): Option[T] = {
      buffer.data.readUInt32() match {
        case 0 => None
        case 1 => Some(buffer.read[T]())
        case other => fail("Invalid tag for option: " + other)
      }
    }
  }

  implicit def pair[T: Readable, U: Readable] = new Readable[(T, U)] {
    def readFrom(buffer: ReadBuffer): (T, U) = {
      (buffer.read[T](), buffer.read[U]())
    }
  }
}

