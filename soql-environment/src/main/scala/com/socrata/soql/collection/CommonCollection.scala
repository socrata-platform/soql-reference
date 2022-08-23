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

  implicit class AugmentedMap[K, V](underlying: Map[K, V]) {
    def mergeWith(that: Map[K, V])(f: (V, V) => V): Map[K, V] = {
      var result = underlying
      for((k, v) <- that) {
        result.get(k) match {
          case Some(v0) =>
            result += k -> f(v0, v)
          case None =>
            result += k -> v
        }
      }
      result
    }
  }
}
