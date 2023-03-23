package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed abstract class JoinType(val debugDoc: Doc[Nothing])
object JoinType {
  case object Inner extends JoinType(d"JOIN")
  case object LeftOuter extends JoinType(d"LEFT OUTER JOIN")
  case object RightOuter extends JoinType(d"RIGHT OUTER JOIN")
  case object FullOuter extends JoinType(d"FULL OUTER JOIN")

  implicit object serialize extends Readable[JoinType] with Writable[JoinType] {
    def writeTo(buffer: WriteBuffer, jt: JoinType): Unit = {
      jt match {
        case Inner => buffer.write(0)
        case LeftOuter => buffer.write(1)
        case RightOuter => buffer.write(2)
        case FullOuter => buffer.write(3)
      }
    }

    def readFrom(buffer: ReadBuffer): JoinType = {
      buffer.read[Int]() match {
        case 0 => Inner
        case 1 => LeftOuter
        case 2 => RightOuter
        case 3 => FullOuter
        case other => fail("Unknown join type tag " + other)
      }
    }
  }
}
