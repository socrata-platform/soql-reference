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
    forwardTables: scm.Map[types.AutoTableLabel[MT], types.AutoTableLabel[MT]],
    backwardTables: scm.Map[types.AutoTableLabel[MT], types.AutoTableLabel[MT]],
    forwardColumns: DMap[(Option[types.AutoTableLabel[MT]], types.ColumnLabel[MT])],
    backwardColumns: DMap[(Option[types.AutoTableLabel[MT]], types.ColumnLabel[MT])]
  ) extends LabelUniverse[MT] {
    def reverse: View[MT] =
      new View(backwardTables, forwardTables, backwardColumns, forwardColumns)

    def renameForward(t: AutoTableLabel): AutoTableLabel = forwardTables.getOrElse(t, t)
    def renameBackward(t: AutoTableLabel): AutoTableLabel = backwardTables.getOrElse(t, t)

    def renameForward[T <: AutoTableLabel, C <: ColumnLabel](t: Option[T], c: C): (Option[T], C) =
      forwardColumns.getOrElse((t, c), (t, c))
    def renameBackward[T <: AutoTableLabel, C <: ColumnLabel](t: Option[T], c: C): (Option[T], C) =
      backwardColumns.getOrElse((t, c), (t, c))
  }
}

private[analyzer2] class IsomorphismState[MT <: MetaTypes] private (
  tableMapLeft: Map[AutoTableLabel, types.DatabaseTableName[MT]],
  tableMapRight: Map[AutoTableLabel, types.DatabaseTableName[MT]],
  forwardTables: scm.Map[types.AutoTableLabel[MT], types.AutoTableLabel[MT]],
  backwardTables: scm.Map[types.AutoTableLabel[MT], types.AutoTableLabel[MT]],
  forwardColumns: IsomorphismState.DMap[(Option[types.AutoTableLabel[MT]], types.ColumnLabel[MT])],
  backwardColumns: IsomorphismState.DMap[(Option[types.AutoTableLabel[MT]], types.ColumnLabel[MT])]
) extends LabelUniverse[MT] {
  def this(
    tableMapLeft: Map[AutoTableLabel, types.DatabaseTableName[MT]],
    tableMapRight: Map[AutoTableLabel, types.DatabaseTableName[MT]]
  ) = this(tableMapLeft, tableMapRight, new scm.HashMap, new scm.HashMap, new IsomorphismState.DMap, new IsomorphismState.DMap)

  def finish = new IsomorphismState.View(forwardTables, backwardTables, forwardColumns, backwardColumns)

  def physicalTableLeft(table: AutoTableLabel): DatabaseTableName = tableMapLeft(table)
  def physicalTableRight(table: AutoTableLabel): DatabaseTableName = tableMapRight(table)

  def tryAssociate(tableA: AutoTableLabel, tableB: AutoTableLabel): Boolean = {
    (tableA, tableB) match {
      case (ta: AutoTableLabel, tb: AutoTableLabel) =>
        tryIntern(ta, tb)
      case _ =>
        false
    }
  }

  def tryAssociate(tableA: Option[AutoTableLabel], columnA: ColumnLabel, tableB: Option[AutoTableLabel], columnB: ColumnLabel): Boolean = {
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
      case _ =>
        false
    }
  }

  private def tryIntern[T <: AutoTableLabel](tableA: T, tableB: T): Boolean = {
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

  private def tryIntern[T <: AutoTableLabel](tableA: Option[T], tableB: Option[T]): Boolean = {
    (tableA, tableB) match {
      case (Some(ta), Some(tb)) => tryIntern(ta, tb)
      case (None, None) => true
      case _ => false
    }
  }

  private def tryIntern[T <: AutoTableLabel, C <: ColumnLabel](tableA: Option[T], columnA: C, tableB: Option[T], columnB: C): Boolean = {
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
