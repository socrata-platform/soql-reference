package com.socrata

// A collection that guarantees the existence of at least one element.
// Fills the same role as scalaz's NonEmptyList, but now we don't have to include scalaz as a dependency,
// and we can use Seq as the internal type (rather than List)
case class NonEmptySeq[T](head: T, tail: Seq[T] = Nil) {
  def length: Int = 1 + tail.length
  def size: Int = length
  def seq: Seq[T] = head +: tail
  def iterator: Iterator[T] = Iterator.single(head) ++ tail.iterator
  def last: T = tail.lastOption.getOrElse(head)
  def reverse: NonEmptySeq[T] = mapSeq(_.reverse)
  def map[U](f: T => U): NonEmptySeq[U] = NonEmptySeq(f(head), tail.map(f))
  def filter(f: T => Boolean): Seq[T] = iterator.filter(f).toSeq
  def flatMap[U](f: T => NonEmptySeq[U]): NonEmptySeq[U] = {
    val first = f(head)
    NonEmptySeq(first.head, first.tail ++ tail.flatMap(f(_).iterator))
  }
  def scanLeft1[U](headF: T => U)(tailF: (U, T) => U): NonEmptySeq[U] = {
    val newHead = headF(head)
    NonEmptySeq(newHead, tail.scanLeft(newHead)(tailF).tail)
  }
  def mapHeadTail(f: (T, Seq[T]) => Seq[T]): NonEmptySeq[T] = {
    NonEmptySeq.fromSeqUnsafe(f(head, tail))
  }
  def mapSeq(f: Seq[T] => Seq[T]): NonEmptySeq[T] = {
    NonEmptySeq.fromSeqUnsafe(f(seq))
  }
}

object NonEmptySeq {
  def fromSeq[T](seq: Seq[T]): Option[NonEmptySeq[T]] = seq match {
    case Seq(h, t@_*) => Some(NonEmptySeq(h, t))
    case _ => None
  }

  def fromSeqUnsafe[T](seq: Seq[T]): NonEmptySeq[T] = {
    fromSeq(seq).getOrElse {
      throw new IllegalArgumentException("cannot create a NonEmptySeq from an empty sequence")
    }
  }
}