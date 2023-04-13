package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed abstract class FrameBound {
  def debugDoc: Doc[Nothing]
}
object FrameBound {
  case object UnboundedPreceding extends FrameBound {
    def debugDoc = d"UNBOUNDED PRECEDING"
  }
  case class Preceding(n: Long) extends FrameBound {
    def debugDoc = d"$n PRECEDING"
  }
  case object CurrentRow extends FrameBound {
    def debugDoc = d"CURRENT ROW"
  }
  case class Following(n: Long) extends FrameBound {
    def debugDoc = d"$n FOLLOWING"
  }
  case object UnboundedFollowing extends FrameBound {
    def debugDoc = d"UNBOUNDED FOLLOWING"
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
