package com.socrata.soql.analyzer2

import scala.collection.compat._

package object rollup {
  implicit class OptionExt[T](private val ts: Option[T]) extends AnyVal {
    def mapFallibly[U](f: T => Option[U]): Option[Option[U]] =
      ts match {
        case None => Some(None)
        case Some(t) => f(t).map(Some(_))
      }
  }

  implicit class IterableOnceExt[T](private val ts: IterableOnce[T]) extends AnyVal {
    def mapFallibly[U](f: T => Option[U]): Option[Seq[U]] = {
      val result = Vector.newBuilder[U]
      for(t <- ts.iterator) {
        f(t) match {
          case Some(u) =>
            result += u
          case None =>
            return None
        }
      }
      Some(result.result())
    }

    def findMap[U](f: T => Option[U]): Option[U] = {
      for(t <- ts.iterator) {
        val maybeResult = f(t)
        if(maybeResult.isDefined) return maybeResult
      }
      None
    }
  }
}
