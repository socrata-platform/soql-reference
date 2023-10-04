package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

case class Frame(
  context: FrameContext,
  start: FrameBound,
  end: Option[FrameBound],
  exclusion: Option[FrameExclusion]
) {
  require(start != FrameBound.UnboundedFollowing)
  for(end <- end) {
    require(end != FrameBound.UnboundedPreceding)
    require(end.level >= start.level)
  }
  def debugDoc =
    Seq(
      Some(context.debugDoc),
      Some(end match {
        case None => start.debugDoc
        case Some(end) =>
          Seq(d"BETWEEN", start.debugDoc, d"AND", end.debugDoc).hsep
      }),
      exclusion.map { e =>
        d"EXCLUDE" +#+ e.debugDoc
      }
    ).flatten.sep
}

object Frame {
  implicit object serialize extends Writable[Frame] with Readable[Frame] {
    def writeTo(buffer: WriteBuffer, t: Frame): Unit = {
      buffer.write(t.context)
      buffer.write(t.start)
      buffer.write(t.end)
      buffer.write(t.exclusion)
    }

    def readFrom(buffer: ReadBuffer): Frame = {
      Frame(
        context = buffer.read[FrameContext](),
        start = buffer.read[FrameBound](),
        end = buffer.read[Option[FrameBound]](),
        exclusion = buffer.read[Option[FrameExclusion]]()
      )
    }
  }
}
