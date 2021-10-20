package com.socrata.soql.parsing

object StreamShim {
  type Stream[+T] = scala.collection.immutable.Stream[T]
  val Stream: scala.collection.immutable.Stream.type = scala.collection.immutable.Stream
}
