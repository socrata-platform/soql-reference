package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}
sealed abstract class FrameExclusion(val debugDoc: Doc[Nothing])
object FrameExclusion {
  case object CurrentRow extends FrameExclusion(d"CURRENT ROW")
  case object Group extends FrameExclusion(d"GROUP")
  case object Ties extends FrameExclusion(d"TIES")
  case object NoOthers extends FrameExclusion(d"NO OTHERS")

  implicit object serialize extends Writable[FrameExclusion] with Readable[FrameExclusion] {
    def writeTo(buffer: WriteBuffer, t: FrameExclusion): Unit = {
      val tag = t match {
        case CurrentRow => 0
        case Group => 1
        case Ties => 2
        case NoOthers => 3
      }
      buffer.write(tag)
    }

    def readFrom(buffer: ReadBuffer): FrameExclusion = {
      buffer.read[Int]() match {
        case 0 => CurrentRow
        case 1 => Group
        case 2 => Ties
        case 3 => NoOthers
        case other => fail("Unknown frame exclusion tag: " + other)
      }
    }
  }
}
