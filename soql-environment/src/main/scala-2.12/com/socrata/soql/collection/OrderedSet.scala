package com.socrata.soql.collection

import scala.collection.immutable._
import scala.collection.generic.{ImmutableSetFactory, CanBuildFrom, GenericCompanion, GenericSetTemplate}
import scala.collection.SetLike

class OrderedSet[A](underlying: Map[A, Int], order: Vector[A])
  extends Set[A] with GenericSetTemplate[A, OrderedSet] with SetLike[A, OrderedSet[A]] with Serializable with IndexedSeqShim[A]
{
  override def companion: GenericCompanion[OrderedSet] = OrderedSet

  override def size: Int = underlying.size

  override def empty = OrderedSet.empty[A]

  def iterator: Iterator[A] = order.iterator

  override def foreach[U](f: A =>  U): Unit = order.foreach(f)

  def contains(e: A): Boolean = underlying.contains(e)

  override def + (e: A): OrderedSet[A] =
    underlying.get(e) match {
      case Some(i) =>
        new OrderedSet(underlying - e + (e -> order.length), order.updated(i, e))
      case None =>
        new OrderedSet(underlying + (e -> order.length), order :+ e)
    }

  override def + (elem1: A, elem2: A, elems: A*): OrderedSet[A] =
    this + elem1 + elem2 ++ elems

  def - (e: A): OrderedSet[A] =
    underlying.get(e) match {
      case Some(idx) =>
        // hmm.
        val newOrdering = order.take(idx) ++ order.drop(idx + 1)
        var i = 0
        var result = new HashMap[A, Int]
        for(elem <- newOrdering) {
          result = result.updated(elem, i)
          i += 1
        }
        new OrderedSet(result, newOrdering)
      case None =>
        this
    }

  override def toSeq = order
}

object OrderedSet extends ImmutableSetFactory[OrderedSet] {
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, OrderedSet[A]] = setCanBuildFrom[A]
  override def empty[A]: OrderedSet[A] = EmptyOrderedSet.asInstanceOf[OrderedSet[A]]
  def emptyInstance: OrderedSet[Any] = EmptyOrderedSet.asInstanceOf[OrderedSet[Any]]

  private val EmptyOrderedSet = new OrderedSet[Any](Map.empty, Vector.empty)
}
