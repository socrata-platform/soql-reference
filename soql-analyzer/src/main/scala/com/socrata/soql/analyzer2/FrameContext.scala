package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed abstract class FrameContext(val debugDoc: Doc[Nothing])
object FrameContext {
  case object Range extends FrameContext(d"RANGE")
  case object Rows extends FrameContext(d"ROWS")
  case object Groups extends FrameContext(d"GROUPS")

  implicit object serialize extends Writable[FrameContext] with Readable[FrameContext] {
    def writeTo(buffer: WriteBuffer, t: FrameContext): Unit = {
      val tag = t match {
        case Range => 0
        case Rows => 1
        case Groups => 2
      }
      buffer.write(tag)
    }

  def readFrom(buffer: ReadBuffer): FrameContext = {
      buffer.read[Int]() match {
        case 0 => Range
        case 1 => Rows
        case 2 => Groups
        case other => fail("Unknown frame context " + other)
      }
    }
  }
}
