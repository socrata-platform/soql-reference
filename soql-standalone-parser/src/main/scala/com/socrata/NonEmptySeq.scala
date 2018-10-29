package com.socrata

// A collection that guarantees the existence of at least one element.
// Fills the same role as scalaz's NonEmptyList, but now we don't have to include scalaz as a dependency,
// and we can use Seq as the internal type (rather than List)
case class NonEmptySeq[+T](head: T, tail: Seq[T] = Seq.empty) {
  def length: Int = 1 + tail.length
  def size: Int = length
  def seq: Seq[T] = head +: tail
  def iterator: Iterator[T] = Iterator.single(head) ++ tail.iterator
  def last: T = tail.lastOption.getOrElse(head)
  def reverse: NonEmptySeq[T] = NonEmptySeq.fromSeqUnsafe(seq.reverse)

  def apply(index: Int): T = {
    if (index == 0) head
    else tail(index - 1)
  }

  def +[TT >: T](t: TT): NonEmptySeq[TT] = NonEmptySeq(head, tail :+ t)
  def ++[TT >: T](other: Iterable[TT]): NonEmptySeq[TT] = NonEmptySeq(head, tail ++ other)
  def ++[TT >: T](other: NonEmptySeq[TT]): NonEmptySeq[TT] = this ++ other.seq
  def prepend[TT >: T](t: TT): NonEmptySeq[TT] = NonEmptySeq(t, seq)
  def prepend[TT >: T](other: Iterable[TT]): NonEmptySeq[TT] = other.toList match {
    case h :: t => NonEmptySeq(h, t ++ seq)
    case Nil => this
  }
  def prepend[TT >: T](other: NonEmptySeq[TT]): NonEmptySeq[TT] = prepend(other.seq)

  def updated[TT >: T](index: Int, t: TT): NonEmptySeq[TT] = NonEmptySeq.fromSeqUnsafe(seq.updated(index, t))
  def replaceLast[TT >: T](t: TT): NonEmptySeq[TT] = updated(length - 1, t)
  def replaceFirst[TT >: T](t: TT): NonEmptySeq[TT] = NonEmptySeq(t, tail)

  def map[U](f: T => U): NonEmptySeq[U] = NonEmptySeq(f(head), tail.map(f))
  def filter(f: T => Boolean): Seq[T] = seq.filter(f)
  def flatMap[U](f: T => NonEmptySeq[U]): NonEmptySeq[U] = {
    val first = f(head)
    NonEmptySeq(first.head, first.tail ++ tail.flatMap(f(_).iterator))
  }
  def scanLeft[U](initial: U)(f: (U, T) => U): NonEmptySeq[U] =
    NonEmptySeq.fromSeqUnsafe(seq.scanLeft(initial)(f))
  def scanLeft1[U](headF: T => U)(tailF: (U, T) => U): NonEmptySeq[U] = {
    val newHead = headF(head)
    NonEmptySeq(newHead, tail.scanLeft(newHead)(tailF).tail)
  }
  /** A foldLeft where, instead of specifying the initial value directly, the initial value is a transform
    * (given by `headF`) of `head` in this NES. If you want to do a regular foldLeft, use .seq.foldLeft
    */
  def foldLeft1[U](headF: T => U)(tailF: (U, T) => U): U = {
    val newHead = headF(head)
    tail.foldLeft(newHead)(tailF)
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