package com.socrata.soql

trait BinaryTree[T] {

  def seq: Seq[T] = {
    asLeaf.toSeq
  }

  /**
   * return: the leaf node of the ultimate schema that this subtree will produce
   */
  def outputSchemaLeaf: T = {
    asLeaf.get
  }

  def last: T = asLeaf.get

  def asLeaf: Option[T] = {
    this match {
      case _: Compound[T] => None
      case t: T @unchecked => Some(t)
    }
  }

  def flatMap[B](transform: T => BinaryTree[B]): BinaryTree[B] = {
    asLeaf match {
      case Some(t) => transform(t)
      case None => ???
    }
  }
}

object Compound {
  def apply[T](op: String, left: BinaryTree[T], right: BinaryTree[T]): Compound[T] = {
    op match {
      case "QUERYPIPE" => PipeQuery(left, right)
      case "UNION" => UnionQuery(left, right)
      case "UNION ALL" => UnionAllQuery(left, right)
      case "INTERSECT" => IntersectQuery(left, right)
      case "MINUS" => MinusQuery(left, right)
    }
  }

  def unapply[T](arg: Compound[T]): Option[(String, BinaryTree[T], BinaryTree[T])] = {
    Some(arg.op, arg.left, arg.right)
  }
}

abstract class Compound[T](val op: String) extends BinaryTree[T] {

  val left: BinaryTree[T]

  val right: BinaryTree[T]

  override def seq: Seq[T] = {
    left.seq ++ right.seq
  }

  override def flatMap[B](transform: T => BinaryTree[B]): BinaryTree[B] = {
    val nl = left.flatMap(transform)
    val nr = right.flatMap(transform)
    Compound(this.op, left = nl, right = nr)
  }

  override def outputSchemaLeaf: T = {
    this match {
      case PipeQuery(_, r) =>
        r.outputSchemaLeaf
      case Compound(_, l, _) =>
        l.outputSchemaLeaf
    }
  }

  override def last: T = {
    right.last
  }

  def opString: String = op

  override def toString: String = {
    if (right.isInstanceOf[Compound[T]]) s"${left.toString} $opString (${right.toString})"
    else s"${left.toString} $opString ${right.toString}"
  }
}

case class PipeQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]("QUERYPIPE") {
  override def opString: String = "|>"
}

case class UnionQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]("UNION")
case class UnionAllQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]("UNION ALL")
case class IntersectQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]("INTERSECT")
case class MinusQuery[T](left: BinaryTree[T], right: BinaryTree[T]) extends Compound[T]("MINUS")
