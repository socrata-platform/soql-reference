package com.socrata.soql.collection

// An immutable-after-construction Set which is covariant in its type
// parameter.
class CovariantSet[+A] private (private val underlying: Set[Any]) {
  def iterator: Iterator[A] = underlying.asInstanceOf[Set[A]].iterator
  def ++[B >: A](that: TraversableOnceLike[B]): CovariantSet[B] = new CovariantSet(underlying ++ that)
  def ++[B >: A](that: CovariantSet[B]): CovariantSet[B] = new CovariantSet(underlying ++ that.underlying)
  def +[B >: A](elem: B): CovariantSet[B] = new CovariantSet[B](underlying + elem)
}

object CovariantSet {
  def apply[A](xs: A*) = new CovariantSet(Set(xs:_*))

  def from[A](xs: Set[A]) = new CovariantSet(xs.iterator.toSet)

  implicit class ElemProxy[A](private val underlying: CovariantSet[A]) extends AnyVal {
    def apply[A](elem: A) = underlying.underlying.contains(elem)
    def -(elem: A) = new CovariantSet[A](underlying.underlying - elem)
    def --(elems: TraversableOnceLike[A]) = new CovariantSet[A](underlying.underlying -- elems)
    def --(elems: CovariantSet[A]) = new CovariantSet[A](underlying.underlying -- elems.underlying)
  }
}
