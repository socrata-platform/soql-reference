package com.socrata.soql.analyzer2

import scala.util.parsing.input.{Position, NoPosition}

import com.socrata.soql.environment.ScopedResourceName
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

sealed abstract class PositionInfo[+RNS] {
  // The difference between logicalPosition and physicalPosition is
  // best described with an example.  Given
  //   SELECT x + y AS z WHERE z > 5
  // then the "z" will have a logical position within the text of
  // the WHERE clause, but a physical position in the select list
  // where it's defined.
  val logicalSource: Option[ScopedResourceName[RNS]]
  val logicalPosition: Position
  val physicalSource: Option[ScopedResourceName[RNS]]
  val physicalPosition: Position

  def logicallyReposition[RNS2 >: RNS](source: Option[ScopedResourceName[RNS2]], pos: Position): PositionInfo[RNS2]

  def asAtomic: AtomicPositionInfo[RNS]
}

final class AtomicPositionInfo[+RNS](
  val logicalSource: Option[ScopedResourceName[RNS]],
  val logicalPosition: Position,
  val physicalSource: Option[ScopedResourceName[RNS]],
  val physicalPosition: Position
) extends PositionInfo[RNS] {
  def this(source: Option[ScopedResourceName[RNS]], position: Position) = this(source, position, source, position)

  def logicallyReposition[RNS2 >: RNS](source: Option[ScopedResourceName[RNS2]], pos: Position) = new AtomicPositionInfo(source, pos, physicalSource, physicalPosition)

  def asAtomic: AtomicPositionInfo[RNS] = this

  override def toString = s"AtomicPositionInfo($logicalSource, $logicalPosition, $physicalSource, $physicalPosition)"
}

object AtomicPositionInfo {
  val None = new AtomicPositionInfo[Nothing](Option.empty, NoPosition)

  implicit def serialize[RNS: Writable] = new Writable[AtomicPositionInfo[RNS]] {
    override def writeTo(buffer: WriteBuffer, api: AtomicPositionInfo[RNS]): Unit = {
      if((api.logicalSource eq api.physicalSource) && (api.logicalPosition eq api.physicalPosition)) {
        // most expressions are not repositioned, so don't duplicate them when serialized if possible
        buffer.write(0)
        buffer.write(api.logicalSource)
        buffer.write(api.logicalPosition)
      } else {
        buffer.write(1)
        buffer.write(api.logicalSource)
        buffer.write(api.logicalPosition)
        buffer.write(api.physicalSource)
        buffer.write(api.physicalPosition)
      }
    }
  }

  implicit def deserialize[RNS: Readable] = new Readable[AtomicPositionInfo[RNS]] {
    override def readFrom(buffer: ReadBuffer): AtomicPositionInfo[RNS] = {
      buffer.read[Int]() match {
        case 0 =>
          new AtomicPositionInfo(
            buffer.read[Option[ScopedResourceName[RNS]]](),
            buffer.read[Position]()
          )
        case 1 =>
          new AtomicPositionInfo(
            logicalSource = buffer.read[Option[ScopedResourceName[RNS]]](),
            logicalPosition = buffer.read[Position](),
            physicalSource = buffer.read[Option[ScopedResourceName[RNS]]](),
            physicalPosition = buffer.read[Position]()
          )
        case other =>
          fail("Unknown AtomicPositionInfo tag " + other)
      }
    }
  }
}

final class FuncallPositionInfo[+RNS](
  val logicalSource: Option[ScopedResourceName[RNS]],
  val logicalPosition: Position,
  val physicalSource: Option[ScopedResourceName[RNS]],
  val physicalPosition: Position,
  val functionNamePosition: Position
) extends PositionInfo[RNS] {
  def this(source: Option[ScopedResourceName[RNS]], position: Position, functionNamePosition: Position) =
    this(source, position, source, position, functionNamePosition)

  override def logicallyReposition[RNS2 >: RNS](source: Option[ScopedResourceName[RNS2]], pos: Position) = new FuncallPositionInfo(source, pos, physicalSource, physicalPosition, functionNamePosition)

  def asAtomic: AtomicPositionInfo[RNS] = new AtomicPositionInfo(logicalSource, logicalPosition, physicalSource, physicalPosition)

  override def toString = s"FuncallPositionInfo($logicalSource, $logicalPosition, $physicalSource, $physicalPosition, $functionNamePosition)"
}

object FuncallPositionInfo {
  val None = new FuncallPositionInfo(Option.empty, NoPosition, Option.empty, NoPosition, NoPosition)

  implicit def serialize[RNS: Writable] = new Writable[FuncallPositionInfo[RNS]] {
    override def writeTo(buffer: WriteBuffer, fpi: FuncallPositionInfo[RNS]): Unit = {
      if((fpi.logicalSource eq fpi.physicalSource) && (fpi.logicalPosition eq fpi.physicalPosition)) {
        // most exprs are not repositioned, so don't duplicate when serialized if possible
        buffer.write(0)
        buffer.write(fpi.logicalSource)
        buffer.write(fpi.logicalPosition)
      } else {
        buffer.write(1)
        buffer.write(fpi.logicalSource)
        buffer.write(fpi.logicalPosition)
        buffer.write(fpi.physicalSource)
        buffer.write(fpi.physicalPosition)
      }
      buffer.write(fpi.functionNamePosition)
    }
  }

  implicit def deserialize[RNS: Readable] = new Readable[FuncallPositionInfo[RNS]] {
    override def readFrom(buffer: ReadBuffer): FuncallPositionInfo[RNS] = {
      val ctor =
        buffer.read[Int]() match {
          case 0 =>
            val source = buffer.read[Option[ScopedResourceName[RNS]]]()
            val pos = buffer.read[Position]()
            new FuncallPositionInfo(source, pos, _ : Position)
          case 1 =>
            val lSource = buffer.read[Option[ScopedResourceName[RNS]]]()
            val lPos = buffer.read[Position]()
            val pSource = buffer.read[Option[ScopedResourceName[RNS]]]()
            val pPos = buffer.read[Position]()
            new FuncallPositionInfo(lSource, lPos, pSource, pPos, _ : Position)
          case other =>
            fail("Unknown FuncallPositionInfo tag " + other)
        }

      ctor(buffer.read[Position]())
    }
  }
}
