package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JValue

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer, Version}

sealed abstract class ColumnHint {
  def orInherited(other: => ColumnHint): ColumnHint
}

object ColumnHint {
  case object Inherited extends ColumnHint {
    def orInherited(other: => ColumnHint) = other
  }

  sealed trait DefiniteHint extends ColumnHint {
    def orInherited(other: => ColumnHint) = this
  }
  case object Absent extends DefiniteHint
  case class Present(value: JValue) extends DefiniteHint

  def fromSourceOption(hint: Option[JValue]): ColumnHint =
    hint match {
      case None => ColumnHint.Inherited
      case Some(v) => ColumnHint.Present(v)
    }

  def fromAnalyzedOption(hint: Option[JValue]): ColumnHint =
    hint match {
      case None => ColumnHint.Absent
      case Some(v) => ColumnHint.Present(v)
    }

  implicit object Serialize extends Writable[ColumnHint] {
    def writeTo(buffer: WriteBuffer, hint: ColumnHint): Unit = {
      hint match {
        case Inherited =>
          buffer.write(0)
        case Present(value) =>
          buffer.write(1)
          buffer.write(value)
        case Absent =>
          // this is last because columnhints in namedexprs used to be
          // Option[JValue], where None meant inherit.  Now we have a
          // new explicit Absent case.
          buffer.write(2)
      }
    }
  }

  implicit object Deserialize extends Readable[ColumnHint] {
    def readFrom(buffer: ReadBuffer): ColumnHint =
      buffer.read[Int]() match {
        case 0 =>
          Inherited
        case 1 =>
          Present(buffer.read[JValue]())
        case 2 =>
          Absent
        case other =>
          fail(s"Invalid tag for column hint: ${other}")
      }
  }
}
