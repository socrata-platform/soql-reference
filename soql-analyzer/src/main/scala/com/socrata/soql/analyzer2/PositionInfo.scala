package com.socrata.soql.analyzer2

import scala.util.parsing.input.{Position, NoPosition}

import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed abstract class PositionInfo {
  // The difference between logicalPosition and physicalPosition is
  // best described with an example.  Given
  //   SELECT x + y AS z WHERE z > 5
  // then the "z" will have a logical position within the text of
  // the WHERE clause, but a physical position in the select list
  // where it's defined.
  val logicalPosition: Position
  val physicalPosition: Position

  def logicallyReposition(pos: Position): PositionInfo

  def asAtomic: AtomicPositionInfo
}

final class AtomicPositionInfo(
  val logicalPosition: Position,
  val physicalPosition: Position
) extends PositionInfo {
  def this(position: Position) = this(position, position)

  def logicallyReposition(pos: Position) = new AtomicPositionInfo(pos, physicalPosition)

  def asAtomic: AtomicPositionInfo = this

  override def toString = s"AtomicPositionInfo($logicalPosition, $physicalPosition)"
}

object AtomicPositionInfo {
  val None = new AtomicPositionInfo(NoPosition)

  implicit object serialize extends Readable[AtomicPositionInfo] with Writable[AtomicPositionInfo] {
    override def writeTo(buffer: WriteBuffer, api: AtomicPositionInfo): Unit = {
      buffer.write(api.logicalPosition)
      buffer.write(api.physicalPosition)
    }

    override def readFrom(buffer: ReadBuffer): AtomicPositionInfo = {
      new AtomicPositionInfo(
        logicalPosition = buffer.read[Position](),
        physicalPosition = buffer.read[Position]()
      )
    }
  }
}

final class FuncallPositionInfo(
  val logicalPosition: Position,
  val physicalPosition: Position,
  val functionNamePosition: Position
) extends PositionInfo {
  def this(position: Position, functionNamePosition: Position) = this(position, position, functionNamePosition)

  override def logicallyReposition(pos: Position) = new FuncallPositionInfo(pos, physicalPosition, functionNamePosition)

  def asAtomic: AtomicPositionInfo = new AtomicPositionInfo(logicalPosition, physicalPosition)

  override def toString = s"FuncallPositionInfo($logicalPosition, $physicalPosition, $functionNamePosition)"
}

object FuncallPositionInfo {
  val None = new FuncallPositionInfo(NoPosition, NoPosition, NoPosition)

  implicit object serialize extends Readable[FuncallPositionInfo] with Writable[FuncallPositionInfo] {
    override def writeTo(buffer: WriteBuffer, fpi: FuncallPositionInfo): Unit = {
      buffer.write(fpi.logicalPosition)
      buffer.write(fpi.physicalPosition)
      buffer.write(fpi.functionNamePosition)
    }

    override def readFrom(buffer: ReadBuffer): FuncallPositionInfo = {
      new FuncallPositionInfo(
        logicalPosition = buffer.read[Position](),
        physicalPosition = buffer.read[Position](),
        functionNamePosition = buffer.read[Position]()
      )
    }
  }
}
