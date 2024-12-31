package com.socrata.soql.analyzer2

import scala.util.parsing.input.{Position, NoPosition}

import com.socrata.soql.environment.{ScopedResourceName, Source}
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer, Version}

sealed abstract class PositionInfo[+RNS] {
  // This is the position of the expression within the source soql
  val source: Source[RNS]
  // This is the position of the _reference_ to the expression within
  // the source soql.  E.g., in
  //   SELECT x + y AS z WHERE z > 5
  // the expression node that is the "z" in the WHERE clause will have
  // a source pointing at the "x + y", but a reference pointing at the
  // location of the "z" in the WHERE clause.
  val reference: Source[RNS]

  def reReference[RNS2 >: RNS](source: Source[RNS2]): PositionInfo[RNS2]

  def asAtomic: AtomicPositionInfo[RNS]
}

final class AtomicPositionInfo[+RNS](
  override val source: Source[RNS],
  override val reference: Source[RNS]
) extends PositionInfo[RNS] {
  def this(source: Source[RNS]) = this(source, source)

  override def reReference[RNS2 >: RNS](newReference: Source[RNS2]) = new AtomicPositionInfo(source, newReference)

  override def asAtomic: AtomicPositionInfo[RNS] = this

  override def toString =
    if(source == reference) {
      s"AtomicPositionInfo($source)"
    } else {
      s"AtomicPositionInfo($source, $reference)"
    }

  override def hashCode = source.hashCode ^ reference.hashCode
  override def equals(o: Any) =
    o match {
      case that: AtomicPositionInfo[_] =>
        this.source == that.source &&
          this.reference == that.reference
      case _ => false
    }
}


object AtomicPositionInfo {
  val Synthetic = new AtomicPositionInfo[Nothing](Source.Synthetic)

  implicit def serialize[RNS: Writable] = new Writable[AtomicPositionInfo[RNS]] {
    override def writeTo(buffer: WriteBuffer, api: AtomicPositionInfo[RNS]): Unit = {
      if(api.source eq api.reference) {
        // most expressions are not repositioned, so don't duplicate them when serialized if possible
        buffer.write(0)
        buffer.write(api.source)
      } else {
        buffer.write(1)
        buffer.write(api.source)
        buffer.write(api.reference)
      }
    }
  }

  implicit def deserialize[RNS: Readable] = new Readable[AtomicPositionInfo[RNS]] {
    override def readFrom(buffer: ReadBuffer): AtomicPositionInfo[RNS] = {
      buffer.version match {
        case Version.V0 =>
          buffer.read[Int]() match {
            case 0 =>
              val pSrn = buffer.read[Option[ScopedResourceName[RNS]]]()
              val pLoc = buffer.read[Position]()
              new AtomicPositionInfo(Source.nonSynthetic(pSrn, pLoc))
            case 1 =>
              val lSrn = buffer.read[Option[ScopedResourceName[RNS]]]()
              val lLoc = buffer.read[Position]()
              val pSrn = buffer.read[Option[ScopedResourceName[RNS]]]()
              val pLoc = buffer.read[Position]()
              new AtomicPositionInfo(
                source = Source.nonSynthetic(pSrn, pLoc),
                reference = Source.nonSynthetic(lSrn, lLoc)
              )
            case other =>
              fail("Unknown AtomicPositionInfo tag " + other)
          }

        case Version.V1 | Version.V2 | Version.V3 =>
          buffer.read[Int]() match {
            case 0 =>
              new AtomicPositionInfo(buffer.read[Source[RNS]]())
            case 1 =>
              new AtomicPositionInfo(
                source = buffer.read[Source[RNS]](),
                reference = buffer.read[Source[RNS]]()
              )
            case other =>
              fail("Unknown AtomicPositionInfo tag " + other)
          }
      }
    }
  }
}

final class FuncallPositionInfo[+RNS](
  val source: Source[RNS],
  val reference: Source[RNS],
  val functionNameSource: Source[RNS]
) extends PositionInfo[RNS] {
  def this(source: Source[RNS], functionNamePosition: Position) =
    this(source, source, source.withPosition(functionNamePosition))

  override def reReference[RNS2 >: RNS](newReference: Source[RNS2]) =
    new FuncallPositionInfo(source, newReference, functionNameSource)

  def asAtomic: AtomicPositionInfo[RNS] =
    new AtomicPositionInfo(source, reference)

  override def toString =
    if(source == reference && source.scopedResourceName == functionNameSource.scopedResourceName) {
      s"FuncallPositionInfo($source, ${functionNameSource.position})"
    } else {
      s"FuncallPositionInfo($source, $reference, $functionNameSource)"
    }
}

object FuncallPositionInfo {
  val Synthetic = new FuncallPositionInfo(Source.Synthetic, Source.Synthetic, Source.Synthetic)

  implicit def serialize[RNS: Writable] = new Writable[FuncallPositionInfo[RNS]] {
    override def writeTo(buffer: WriteBuffer, fpi: FuncallPositionInfo[RNS]): Unit = {
      if(fpi.source eq fpi.reference) {
        // most exprs are not repositioned, so don't duplicate when serialized if possible
        buffer.write(0)
        buffer.write(fpi.source)
      } else {
        buffer.write(1)
        buffer.write(fpi.source)
        buffer.write(fpi.reference)
      }
      buffer.write(fpi.functionNameSource)
    }
  }

  implicit def deserialize[RNS: Readable] = new Readable[FuncallPositionInfo[RNS]] {
    override def readFrom(buffer: ReadBuffer): FuncallPositionInfo[RNS] = {
      buffer.version match {
        case Version.V0 =>
          val ctor =
            buffer.read[Int]() match {
              case 0 =>
                val srn = buffer.read[Option[ScopedResourceName[RNS]]]()
                val pos = buffer.read[Position]()
                new FuncallPositionInfo(Source.nonSynthetic(srn, pos), _ : Position)
              case 1 =>
                val lSrn = buffer.read[Option[ScopedResourceName[RNS]]]()
                val lPos = buffer.read[Position]()
                val pSrn = buffer.read[Option[ScopedResourceName[RNS]]]()
                val pPos = buffer.read[Position]()

                { fnPos: Position =>
                  new FuncallPositionInfo(
                    source = Source.nonSynthetic(pSrn, pPos),
                    reference = Source.nonSynthetic(lSrn, lPos),
                    functionNameSource = Source.nonSynthetic(pSrn, fnPos)
                  )
                }
              case other =>
                fail("Unknown FuncallPositionInfo tag " + other)
            }
          ctor(buffer.read[Position]())

        case Version.V1 | Version.V2 | Version.V3 =>
          val ctor =
            buffer.read[Int]() match {
              case 0 =>
                val source = buffer.read[Source[RNS]]()
                new FuncallPositionInfo(source, source, _ : Source[RNS])
              case 1 =>
                val source = buffer.read[Source[RNS]]()
                val reference = buffer.read[Source[RNS]]()

                { (fnSrc: Source[RNS]) =>
                  new FuncallPositionInfo(
                    source = source,
                    reference = reference,
                    functionNameSource = fnSrc
                  )
                }
              case other =>
                fail("Unknown FuncallPositionInfo tag " + other)
            }

          ctor(buffer.read[Source[RNS]]())
      }
    }
  }
}
