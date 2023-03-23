package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed abstract class TableFunc(val debugDoc: Doc[Nothing])
object TableFunc {
  case object Union extends TableFunc(d"UNION")
  case object UnionAll extends TableFunc(d"UNION ALL")
  case object Intersect extends TableFunc(d"INTERSECT")
  case object IntersectAll extends TableFunc(d"INTERSECT ALL")
  case object Minus extends TableFunc(d"MINUS")
  case object MinusAll extends TableFunc(d"MINUS ALL")

  implicit object serialize extends Readable[TableFunc] with Writable[TableFunc] {
    def writeTo(buffer: WriteBuffer, tf: TableFunc): Unit = {
      val tag =
        tf match {
          case Union => 0
          case UnionAll => 1
          case Intersect => 2
          case IntersectAll => 3
          case Minus => 4
          case MinusAll => 5
        }
      buffer.write(tf)
    }

    def readFrom(buffer: ReadBuffer) =
      buffer.read[Int]() match {
        case 0 => Union
        case 1 => UnionAll
        case 2 => Intersect
        case 3 => IntersectAll
        case 4 => Minus
        case 5 => MinusAll
        case other => fail("Unknown table func tag " + other)
      }
  }
}
