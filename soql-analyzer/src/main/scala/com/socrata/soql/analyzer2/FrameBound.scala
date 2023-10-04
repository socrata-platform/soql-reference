package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed abstract class FrameBound {
  def text: String
  def debugDoc: Doc[Nothing] = Doc(text)
  val level: Int // an end bound is not allowed to have a lower level than a start bound
}
object FrameBound {
  case object UnboundedPreceding extends FrameBound {
    def text = "UNBOUNDED PRECEDING"
    val level = 0
  }
  case class Preceding(n: Long) extends FrameBound {
    def text = s"$n PRECEDING"
    val level = 1
  }
  case object CurrentRow extends FrameBound {
    def text = "CURRENT ROW"
    val level = 2
  }
  case class Following(n: Long) extends FrameBound {
    def text = s"$n FOLLOWING"
    val level = 3
  }
  case object UnboundedFollowing extends FrameBound {
    def text = "UNBOUNDED FOLLOWING"
    val level = 4
  }

  implicit object serialize extends Writable[FrameBound] with Readable[FrameBound] {
    def writeTo(buffer: WriteBuffer, t: FrameBound): Unit = {
      t match {
        case UnboundedPreceding =>
          buffer.write(0)
        case Preceding(n) =>
          buffer.write(1)
          buffer.write(n)
        case CurrentRow =>
          buffer.write(2)
        case Following(n) =>
          buffer.write(3)
          buffer.write(n)
        case UnboundedFollowing =>
          buffer.write(4)
      }
    }

    def readFrom(buffer: ReadBuffer): FrameBound = {
      buffer.read[Int]() match {
        case 0 => UnboundedPreceding
        case 1 => Preceding(buffer.read[Long]())
        case 2 => CurrentRow
        case 3 => Following(buffer.read[Long]())
        case 4 => UnboundedFollowing
        case other => fail("Unknown frame bound tag: " + other)
      }
    }
  }
}
