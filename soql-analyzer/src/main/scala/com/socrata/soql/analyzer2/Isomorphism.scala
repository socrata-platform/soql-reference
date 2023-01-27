package com.socrata.soql.analyzer2

import scala.collection.{mutable => scm}

trait IsomorphismUpToAutoLabels[+T] {
  val leftAsRight: T
  val rightAsLeft: T
}

private[analyzer2] object IsomorphismState {
  // This is not actually sound, which is why I'm not putting it in
  // the collections package.  In particular, you will get crashes if
  // you have:
  //
  // class A
  // class B extends A
  //
  // val dmap = new DMap[A]
  // dmap += someB -> new A
  // dmap.get(someB) // will try to return a Some[B] => explode
  //
  // however, this code verifies that all inserted pairs have the same
  // dynamic type (since the label hierarchy is completely sealed) so
  // that can't happen (at time of writing anyway).
  class DMap[A] {
    private val underlying = new scm.HashMap[A, A]

    def +=[B <: A](that: (B, B)): Unit = {
      underlying += that
    }

    def getOrElse[B <: A](k: B, otherwise: B): B =
      underlying.getOrElse(k, otherwise).asInstanceOf[B]

    def get[B <: A](k: B): Option[B] =
      underlying.get(k).asInstanceOf[Option[B]]
  }

  private[analyzer2] class View[MT <: MetaTypes] private[IsomorphismState](
    forwardTables: DMap[TableLabel[MT#DatabaseTableNameImpl]],
    backwardTables: DMap[TableLabel[MT#DatabaseTableNameImpl]],
    forwardColumns: DMap[(Option[TableLabel[MT#DatabaseTableNameImpl]], ColumnLabel[MT#DatabaseColumnNameImpl])],
    backwardColumns: DMap[(Option[TableLabel[MT#DatabaseTableNameImpl]], ColumnLabel[MT#DatabaseColumnNameImpl])]
  ) extends LabelHelper[MT] {
    def reverse: View[MT] =
      new View(backwardTables, forwardTables, backwardColumns, forwardColumns)

    def renameForward[T <: TableLabel](t: T): T = forwardTables.getOrElse(t, t)
    def renameBackward[T <: TableLabel](t: T): T = backwardTables.getOrElse(t, t)

    def renameForward[T <: TableLabel, C <: ColumnLabel](t: Option[T], c: C): (Option[T], C) =
      forwardColumns.getOrElse((t, c), (t, c))
    def renameBackward[T <: TableLabel, C <: ColumnLabel](t: Option[T], c: C): (Option[T], C) =
      backwardColumns.getOrElse((t, c), (t, c))
  }
}

private[analyzer2] class IsomorphismState[MT <: MetaTypes] private (
  forwardTables: IsomorphismState.DMap[TableLabel[MT#DatabaseTableNameImpl]],
  backwardTables: IsomorphismState.DMap[TableLabel[MT#DatabaseTableNameImpl]],
  forwardColumns: IsomorphismState.DMap[(Option[TableLabel[MT#DatabaseTableNameImpl]], ColumnLabel[MT#DatabaseColumnNameImpl])],
  backwardColumns: IsomorphismState.DMap[(Option[TableLabel[MT#DatabaseTableNameImpl]], ColumnLabel[MT#DatabaseColumnNameImpl])]
) extends LabelHelper[MT] {
  def this() = this(new IsomorphismState.DMap,new IsomorphismState.DMap,new IsomorphismState.DMap,new IsomorphismState.DMap)

  def finish = new IsomorphismState.View(forwardTables, backwardTables, forwardColumns, backwardColumns)

  def tryAssociate(tableA: TableLabel, tableB: TableLabel): Boolean = {
    (tableA, tableB) match {
      case (ta: AutoTableLabel, tb: AutoTableLabel) =>
        tryIntern(ta, tb)
      case (ta@DatabaseTableName(a), tb@DatabaseTableName(b)) if a == b =>
        tryIntern(ta, tb)
      case _ =>
        false
    }
  }

  def tryAssociate(tableA: Option[TableLabel], columnA: ColumnLabel, tableB: Option[TableLabel], columnB: ColumnLabel): Boolean = {
    (tableA, tableB) match {
      case (ta@None, tb@None) =>
        (columnA, columnB) match {
          case (ca: AutoColumnLabel, cb: AutoColumnLabel) =>
            tryIntern(ta, ca, tb, cb)
          case (ca@DatabaseColumnName(a), cb@DatabaseColumnName(b)) if a == b =>
            tryIntern(ta, ca, tb, cb)
          case _ =>
            false
        }
      case (ta@Some(_ : AutoTableLabel), tb@Some(_ : AutoTableLabel)) =>
        (columnA, columnB) match {
          case (ca: AutoColumnLabel, cb: AutoColumnLabel) =>
            tryIntern(ta, ca, tb, cb)
          case (ca@DatabaseColumnName(a), cb@DatabaseColumnName(b)) if a == b =>
            tryIntern(ta, ca, tb, cb)
          case _ =>
            false
        }
      case (ta@Some(DatabaseTableName(a)), tb@Some(DatabaseTableName(b))) if a == b =>
        (columnA, columnB) match {
          case (ca: AutoColumnLabel, cb: AutoColumnLabel) =>
            tryIntern(ta, ca, tb, cb)
          case (ca@DatabaseColumnName(a), cb@DatabaseColumnName(b)) if a == b =>
            tryIntern(ta, ca, tb, cb)
          case _ =>
            false
        }
      case _ =>
        false
    }
  }

  private def tryIntern[T <: TableLabel](tableA: T, tableB: T): Boolean = {
    (forwardTables.get(tableA), backwardTables.get(tableB)) match {
      case (None, None) =>
        forwardTables += tableA -> tableB
        backwardTables += tableB -> tableA
        true
      case (Some(b), Some(a)) =>
        a == tableA && b == tableB
      case _ =>
        false
    }
  }

  private def tryIntern[T <: TableLabel](tableA: Option[T], tableB: Option[T]): Boolean = {
    (tableA, tableB) match {
      case (Some(ta), Some(tb)) => tryIntern(ta, tb)
      case (None, None) => true
      case _ => false
    }
  }

  private def tryIntern[T <: TableLabel, C <: ColumnLabel](tableA: Option[T], columnA: C, tableB: Option[T], columnB: C): Boolean = {
    (forwardColumns.get((tableA, columnA)), backwardColumns.get((tableB, columnB))) match {
      case (None, None) if tryIntern(tableA, tableB) =>
        forwardColumns += (tableA, columnA) -> (tableB, columnB)
        backwardColumns += (tableB, columnB) -> (tableA, columnA)
        true
      case (Some((tb, cb)), Some((ta, ca))) =>
        ta == tableA && tb == tableB && ca == columnA && cb == columnB
      case _ =>
        false
    }
  }
}
