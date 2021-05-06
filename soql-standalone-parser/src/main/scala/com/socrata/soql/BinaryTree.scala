package com.socrata.soql

sealed trait BinaryTree[T] {

  def seq: Seq[T] = {
    asLeaf.toSeq
  }

  /**
   * return: the leaf node of the ultimate schema that this subtree will produce
   * TODO: Retire this in favor of outputSchema
   */
  @Deprecated
  def outputSchemaLeaf: T = {
    asLeaf.get
  }

  /**
   * return: the leaf of the ultimate schema that this subtree will produce
   */
  def outputSchema: Leaf[T]

  def last: T = asLeaf.get

  def asLeaf: Option[T] = {
    this match {
      case _: Compound[T] => None
      case Leaf(t) => Some(t)
    }
  }

  def leftMost: Leaf[T]

  def flatMap[B](transform: T => BinaryTree[B]): BinaryTree[B]

  def replace(a: Leaf[T], b: Leaf[T]): BinaryTree[T] = {
    this match {
      case Compound(op, l, r) =>
        val nl = l.replace(a, b)
        val nr = r.replace(a, b)
        Compound(op, left = nl, right = nr)
      case Leaf(_) =>
        if (this.eq(a)) b
        else this
    }
  }

  def map[U](f: T => U): BinaryTree[U] = {
    this match {
      case Compound(op, l, r) =>
        val nl = l.map(f)
        val nr = r.map(f)
        Compound(op, left = nl, right = nr)
      case Leaf(t) =>
        Leaf(f(t))
    }
  }
}

object Compound {
  val QueryPipe = "QUERYPIPE"
  val Union = "UNION"
  val UnionAll = "UNION ALL"
  val Intersect = "INTERSECT"
  val Minus = "MINUS"

  def apply[T](op: String, left: BinaryTree[T], right: BinaryTree[T]): Compound[T] = {
    op match {
      case QueryPipe => PipeQuery(left, right)
      case Union => UnionQuery(left, right)
      case UnionAll => UnionAllQuery(left, right)
      case Intersect => IntersectQuery(left, right)
      case Minus => MinusQuery(left, right)
    }
  }

  def unapply[T](arg: Compound[T]): Option[(String, BinaryTree[T], BinaryTree[T])] = {
    Some(arg.op, arg.left, arg.right)
  }
}

sealed trait Compound[T] extends BinaryTree[T] {

  val op: String

  val left: BinaryTree[T]

  val right: BinaryTree[T]

  override def seq: Seq[T] = {
    left.seq ++ right.seq
  }

  def flatMap[B](transform: T => BinaryTree[B]): BinaryTree[B] = {
    val nl = left.flatMap(transform)
    val nr = right.flatMap(transform)
    Compound(this.op, left = nl, right = nr)
  }

  override def outputSchemaLeaf: T = {
    outputSchema.leaf
  }

  override def outputSchema: Leaf[T] = {
    this match {
      case PipeQuery(_, r) =>
        r.outputSchema
      case Compound(_, l, _) =>
        l.outputSchema
    }
  }

  override def last: T = {
    right.last
  }

  def leftMost: Leaf[T] = left.leftMost

  def opString: String = op

  override def toString: String = {
    if (right.isInstanceOf[Compound[T]]) s"${left.toString} $opString (${right.toString})"
    else s"${left.toString} $opString ${right.toString}"
  }
}

case class PipeQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]() {
  override def opString: String = "|>"

  val op = Compound.QueryPipe
}

case class UnionQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]() {
  val op = Compound.Union
}

case class UnionAllQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]() {
  val op = Compound.UnionAll
}

case class IntersectQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]() {
  val op = Compound.Intersect
}

case class MinusQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]() {
  val op = Compound.Minus
}

case class Leaf[T](leaf: T) extends BinaryTree[T] {
  override def toString: String = leaf.toString

  def flatMap[B](transform: T => BinaryTree[B]): BinaryTree[B] = {
    transform(leaf)
  }

  def leftMost: Leaf[T] = this

  def outputSchema: Leaf[T] = this
}
