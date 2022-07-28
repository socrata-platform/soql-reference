package com.socrata.soql.collection

abstract class CommonCollection {
  implicit class AugmentedIterable[A](underlying: Iterable[A]) {
    def findMap[B](f: A => Option[B]): Option[B] = {
      for(elem <- underlying.iterator) {
        val candidate = f(elem)
        if(candidate.isDefined) {
          return candidate
        }
      }
      None
    }
  }
}
