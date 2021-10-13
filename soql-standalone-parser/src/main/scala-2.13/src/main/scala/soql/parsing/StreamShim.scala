package com.socrata.soql.parsing

object StreamShim {
  type Stream[+T] = LazyList[T]
  val Stream: LazyList.type = LazyList
}
