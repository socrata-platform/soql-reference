package com.socrata.soql

import scala.collection.IterableView

package object collection extends collection.CommonCollection {
  type TraversableOnceLike[+A] = TraversableOnce[A]

  implicit class AugmentedIterableView[A, B](underlying: IterableView[(A, B), Map[A, B]]) {
    def filterKeys(f: A => Boolean): IterableView[(A, B), Map[A, B]] =
      underlying.filter { case (a, _) => f(a) }

    def mapValues[C](f: B => C): IterableView[(A, C), Iterable[_]] =
      underlying.map { case (a, b) => (a, f(b)) }
  }
}
