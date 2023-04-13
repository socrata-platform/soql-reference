package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed abstract class MaterializedHint(val debugDoc: Option[Doc[Nothing]])
object MaterializedHint {
  case object Default extends MaterializedHint(None)
  case object Materialized extends MaterializedHint(Some(d"MATERIALIZED"))
  case object NotMaterialized extends MaterializedHint(Some(d"NOT MATERIALIZED"))

  implicit object serialize extends Readable[MaterializedHint] with Writable[MaterializedHint] {
    def writeTo(buffer: WriteBuffer, mh: MaterializedHint): Unit = {
      val tag =
        mh match {
          case Default => 0
          case Materialized => 1
          case NotMaterialized => 2
        }
      buffer.write(tag)
    }

    def readFrom(buffer: ReadBuffer): MaterializedHint =
      buffer.read[Int]() match {
        case 0 => Default
        case 1 => Materialized
        case 2 => NotMaterialized
        case other => fail("Unknown materialized hint tag " + other)
      }
  }
}
