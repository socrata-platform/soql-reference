package com.socrata.soql.analyzer2

import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed trait SelectHint
object SelectHint {
  case object Materialized extends SelectHint
  case object NoRollup extends SelectHint
  case object NoChainMerge extends SelectHint
  case object CompoundRollup extends SelectHint
  case object RollupAtJoin extends SelectHint

  implicit object serialize extends Writable[SelectHint] with Readable[SelectHint] {
    def writeTo(buffer: WriteBuffer, hint: SelectHint): Unit = {
      val tag = hint match {
        case Materialized => 0
        case NoRollup => 1
        case NoChainMerge => 2
        case CompoundRollup => 3
        case RollupAtJoin => 4
      }

      buffer.write(tag)
    }

    def readFrom(buffer: ReadBuffer): SelectHint = {
      buffer.read[Int]() match {
        case 0 => Materialized
        case 1 => NoRollup
        case 2 => NoChainMerge
        case 3 => CompoundRollup
        case 4 => RollupAtJoin
        case other => fail("Invalid select hint tag " + other)
      }
    }
  }
}
