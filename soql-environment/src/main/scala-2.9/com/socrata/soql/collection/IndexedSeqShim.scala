package com.socrata.soql.collection

trait IndexedSeqShim[A] { self: OrderedSet[A] =>
  override def toIndexedSeq[B >: A] = toSeq
}
