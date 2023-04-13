package com.socrata.soql

import scala.collection.immutable.{ListMap, ListSet}

package object collection extends collection.CommonCollection {
  type TraversableOnceLike[+A] = IterableOnce[A]

  type OrderedMap[K, +V] = ListMap[K, V]
  val OrderedMap: ListMap.type = ListMap

  type OrderedSet[A] = ListSet[A]
  val OrderedSet: ListSet.type = ListSet

  implicit class AugmentedOrderedMap[K, V](private val underlying: OrderedMap[K, V]) extends AnyVal {
    def withValuesMapped[V2](f: V => V2): OrderedMap[K, V2] =
      underlying.iterator.map { case (k, v) => k -> f(v) }.to(OrderedMap)
  }
}
