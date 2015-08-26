package com.socrata.soql.collection

import scala.collection.immutable.{MapLike, HashMap}
import scala.collection.generic.{CanBuildFrom, ImmutableMapFactory}

class OrderedMap[A, +B](underlying: Map[A, (Int, B)], ordering: Vector[A]) extends Map[A,B] with MapLike[A, B, OrderedMap[A, B]] with Serializable {

  override def size: Int = underlying.size

  override def empty = OrderedMap.empty[A, B]

  def iterator: Iterator[(A,B)] = ordering.iterator.map { a =>
    a -> underlying(a)._2
  }

  override def mapValues[C](f: B => C): OrderedMap[A, C] = new OrderedMap(underlying.mapValues { case (k,v) => (k,f(v)) }, ordering)

  override def foreach[U](f: ((A, B)) =>  U): Unit = iterator.foreach(f)

  def get(key: A): Option[B] =
    underlying.get(key).map(_._2)

  override def updated [B1 >: B] (key: A, value: B1): OrderedMap[A, B1] =
    underlying.get(key) match {
      case Some((idx, _)) =>
        new OrderedMap(underlying - key + (key -> ((idx, value))), ordering.updated(idx, key))
      case None =>
        new OrderedMap(underlying.updated(key, (ordering.length, value)), ordering :+ key)
    }

  override def + [B1 >: B] (kv: (A, B1)): OrderedMap[A, B1] =
    updated(kv._1, kv._2)

  override def + [B1 >: B] (elem1: (A, B1), elem2: (A, B1), elems: (A, B1) *): OrderedMap[A, B1] =
    this + elem1 + elem2 ++ elems

  def - (key: A): OrderedMap[A, B] =
    underlying.get(key) match {
      case Some((idx, _)) =>
        // hmm.
        val newOrdering = ordering.take(idx) ++ ordering.drop(idx + 1)
        var i = 0
        var result = new HashMap[A, (Int, B)]
        for(elem <- newOrdering) {
          result = result.updated(elem, (i, underlying(elem)._2))
          i += 1
        }
        new OrderedMap(result, newOrdering)
      case None =>
        this
    }

  override def keySet: OrderedSet[A] = new OrderedSet(underlying.mapValues(_._1), ordering)
}

object OrderedMap extends ImmutableMapFactory[OrderedMap] {
  implicit def canBuildFrom[A, B]: CanBuildFrom[Coll, (A, B), OrderedMap[A, B]] = new MapCanBuildFrom[A, B]
  def empty[A, B]: OrderedMap[A, B] = EmptyOrderedMap.asInstanceOf[OrderedMap[A, B]]

  private val EmptyOrderedMap = new OrderedMap[Any, Any](HashMap.empty, Vector.empty)
}
