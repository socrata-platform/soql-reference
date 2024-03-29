package com.socrata

import com.socrata.soql.collection.{NonEmptySeq => NES}

@deprecated(message = "Use com.socrata.soql.collections.NonEmptySeq instead", since="4.9.0")
class NonEmptySeq[+T](head: T, tail: Seq[T] = Seq.empty) extends NES(head, tail)

@deprecated(message = "Use com.socrata.soql.collections.NonEmptySeq instead", since="4.9.0")
object NonEmptySeq {
  def fromSeq[T](seq: Seq[T]): Option[NonEmptySeq[T]] =
    NES.fromSeq(seq).map { case NES(hd, tl) =>
      new NonEmptySeq(hd, tl)
    }

  def fromSeqUnsafe[T](seq: Seq[T]): NonEmptySeq[T] = {
    val NES(hd, tl) = NES.fromSeqUnsafe(seq)
    new NonEmptySeq(hd, tl)
  }
}
