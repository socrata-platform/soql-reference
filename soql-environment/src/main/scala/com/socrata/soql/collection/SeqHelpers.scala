package com.socrata.soql.collection

import scala.collection.immutable.ListMap

object SeqHelpers {
  implicit class addMapAccum[T](ts: Seq[T]) {
    def mapAccum[S, U](init: S)(f: (S, T) => (S, U)): (S, Seq[U]) = {
      val result = ts.companion.newBuilder[U]
      var s = init
      for(t <- ts) {
        val (s1, u) = f(s, t)
        s = s1
        result += u
      }
      (s, result.result())
    }
  }

  implicit class addMapAccumOpt[T](opt: Option[T]) {
    def mapAccum[S, U](init: S)(f: (S, T) => (S, U)): (S, Option[U]) = {
      opt match {
        case Some(v) =>
          val (s1, u) = f(init, v)
          (s1, Some(u))
        case None =>
          (init, None)
      }
    }
  }

  implicit class addMapAccumMap[K, V](m: Map[K, V]) {
    def mapAccumValues[S, U](init: S)(f: (S, V) => (S, U)): (S, Map[K, U]) = {
      val result = ListMap.newBuilder[K, U]
      var s = init
      for((k, v) <- m) {
        val (s1, u) = f(s, v)
        s = s1
        result += (k -> u)
      }
      (s, result.result())
    }
  }
}
