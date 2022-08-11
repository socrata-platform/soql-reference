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

  private[analyzer2] class View private[IsomorphismState](
    forwardTables: DMap[TableLabel],
    backwardTables: DMap[TableLabel],
    forwardColumns: DMap[(TableLabel, ColumnLabel)],
    backwardColumns: DMap[(TableLabel, ColumnLabel)]
  ) {
    def reverse: View =
      new View(backwardTables, forwardTables, backwardColumns, forwardColumns)

    def renameForward[T <: TableLabel](t: T): T = forwardTables.getOrElse(t, t)
    def renameBackward[T <: TableLabel](t: T): T = backwardTables.getOrElse(t, t)

    def renameForward[T <: TableLabel, C <: ColumnLabel](t: T, c: C): (T, C) =
      forwardColumns.getOrElse((t, c), (t, c))
    def renameBackward[T <: TableLabel, C <: ColumnLabel](t: T, c: C): (T, C) =
      backwardColumns.getOrElse((t, c), (t, c))
  }
}

private[analyzer2] class IsomorphismState private (
  forwardTables: IsomorphismState.DMap[TableLabel],
  backwardTables: IsomorphismState.DMap[TableLabel],
  forwardColumns: IsomorphismState.DMap[(TableLabel, ColumnLabel)],
  backwardColumns: IsomorphismState.DMap[(TableLabel, ColumnLabel)]
) {
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

  def tryAssociate(tableA: TableLabel, columnA: ColumnLabel, tableB: TableLabel, columnB: ColumnLabel): Boolean = {
    (tableA, tableB) match {
      case (ta: AutoTableLabel, tb: AutoTableLabel) =>
        (columnA, columnB) match {
          case (ca: AutoColumnLabel, cb: AutoColumnLabel) =>
            tryIntern(ta, ca, tb, cb)
          case (ca@DatabaseColumnName(a), cb@DatabaseColumnName(b)) if a == b =>
            tryIntern(ta, ca, tb, cb)
          case _ =>
            false
        }
      case (ta@DatabaseTableName(a), tb@DatabaseTableName(b)) if a == b =>
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

  private def tryIntern[T <: TableLabel, C <: ColumnLabel](tableA: T, columnA: C, tableB: T, columnB: C): Boolean = {
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
