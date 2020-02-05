package com.socrata.soql.collection

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
}
