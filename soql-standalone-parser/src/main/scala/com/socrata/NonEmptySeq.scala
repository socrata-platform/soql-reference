package com.socrata

case class NonEmptySeq[T](head: T, tail: Seq[T] = Nil) {
  def length: Int = 1 + tail.length
  def size: Int = length
  def seq: Seq[T] = head +: tail
  def iterator: Iterator[T] = Iterator.single(head) ++ tail.iterator
  def last: T = tail.lastOption.getOrElse(head)
  def reverse: NonEmptySeq[T] = NonEmptySeq.fromSeqUnsafe(seq.reverse)
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
}

object NonEmptySeq {
  def fromSeq[T](seq: Seq[T]): Option[NonEmptySeq[T]] = seq match {
    case Seq(h, t@_*) => Some(NonEmptySeq(h, t))
    case _ => None
  }

  def fromSeqUnsafe[T](seq: Seq[T]): NonEmptySeq[T] = fromSeq(seq).get
}