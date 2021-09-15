package com.socrata.soql

sealed trait BinaryTree[T] {

  def wrapInParen: BinaryTree[T]

  def inParen: Boolean

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
      case Leaf(t, _) => Some(t)
    }
  }

  def leftMost: Leaf[T]

  def flatMap[B](transform: T => BinaryTree[B]): BinaryTree[B]

  def replace(a: Leaf[T], b: Leaf[T]): BinaryTree[T] = {
    this match {
      case c@Compound(op, l, r) =>
        val nl = l.replace(a, b)
        val nr = r.replace(a, b)
        Compound(op, left = nl, right = nr, c.inParen)
      case Leaf(_, _) =>
        if (this.eq(a)) b
        else this
    }
  }

  def map[U](f: T => U): BinaryTree[U] = {
    this match {
      case c@Compound(op, l, r) =>
        val nl = l.map(f)
        val nr = r.map(f)
        Compound(op, left = nl, right = nr, c.inParen)
      case Leaf(t, inParen) =>
        Leaf(f(t), inParen)
    }
  }

  def foreach[U](f: T => U): Unit = {
    this match {
      case Compound(_, l, r) =>
        l.foreach(f)
        r.foreach(f)
      case Leaf(t, _) =>
        f(t)
    }
  }
}

object Compound {
  val QueryPipe = "QUERYPIPE"
  val Union = "UNION"
  val UnionAll = "UNION ALL"
  val Intersect = "INTERSECT"
  val IntersectAll = "INTERSECT ALL"
  val Minus = "MINUS"
  val MinusAll = "MINUS ALL"

  def apply[T](op: String, left: BinaryTree[T], right: BinaryTree[T], inParen: Boolean): Compound[T] = {
    op match {
      case QueryPipe => PipeQuery(left, right, inParen)
      case Union => UnionQuery(left, right, inParen)
      case UnionAll => UnionAllQuery(left, right, inParen)
      case Intersect => IntersectQuery(left, right, inParen)
      case IntersectAll => IntersectAllQuery(left, right, inParen)
      case Minus => MinusQuery(left, right, inParen)
      case MinusAll => MinusAllQuery(left, right, inParen)
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

  val inParen: Boolean

  def wrapInParen(): BinaryTree[T] = {
    Compound(op, left, right, true)
  }

  override def seq: Seq[T] = {
    left.seq ++ right.seq
  }

  def flatMap[B](transform: T => BinaryTree[B]): BinaryTree[B] = {
    val nl = left.flatMap(transform)
    val nr = right.flatMap(transform)
    Compound(this.op, left = nl, right = nr, inParen)
  }

  override def outputSchemaLeaf: T = {
    outputSchema.leaf
  }

  override def outputSchema: Leaf[T] = {
    this match {
      case PipeQuery(_, r, _) =>
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
    val s = if (right.isInstanceOf[Compound[T]] && !right.inParen) s"${left.toString} $opString (${right.toString})"
            else s"${left.toString} $opString ${right.toString}"
    if (inParen) s"(${s})"
    else s
  }
}

case class PipeQuery[T](left: BinaryTree[T], right: BinaryTree[T], inParen: Boolean = false) extends Compound[T]() {
  override def opString: String = "|>"

  val op = Compound.QueryPipe
}

case class UnionQuery[T](left: BinaryTree[T], right: BinaryTree[T], inParen: Boolean = false) extends Compound[T]() {
  val op = Compound.Union
}

case class UnionAllQuery[T](left: BinaryTree[T], right: BinaryTree[T], inParen: Boolean = false) extends Compound[T]() {
  val op = Compound.UnionAll
}

case class IntersectQuery[T](left: BinaryTree[T], right: BinaryTree[T], inParen: Boolean = false) extends Compound[T]() {
  val op = Compound.Intersect
}

case class IntersectAllQuery[T](left: BinaryTree[T], right: BinaryTree[T], inParen: Boolean = false) extends Compound[T]() {
  val op = Compound.IntersectAll
}

case class MinusQuery[T](left: BinaryTree[T], right: BinaryTree[T], inParen: Boolean = false) extends Compound[T]() {
  val op = Compound.Minus
}

case class MinusAllQuery[T](left: BinaryTree[T], right: BinaryTree[T], inParen: Boolean = false) extends Compound[T]() {
  val op = Compound.MinusAll
}

case class Leaf[T](leaf: T, inParen: Boolean = false) extends BinaryTree[T] {
  override def toString: String = {
    if (inParen) s"(${leaf.toString})"
    else leaf.toString
  }

  def wrapInParen(): BinaryTree[T] = {
    Leaf(leaf, true)
  }

  def flatMap[B](transform: T => BinaryTree[B]): BinaryTree[B] = {
    transform(leaf)
  }

  def leftMost: Leaf[T] = this

  def outputSchema: Leaf[T] = this
}
