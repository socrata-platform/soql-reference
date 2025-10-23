package com.socrata.soql.analyzer2

trait IsomorphismUpToAutoLabels[+T] {
  val leftAsRight: T
  val rightAsLeft: T
}

object IsomorphismState {
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
  private class DMap[A] {
    private var underlying = Map.empty[A, A]

    def +=[B <: A](that: (B, B)): Unit = {
      underlying += that
    }

    def getOrElse[B <: A](k: B, otherwise: B): B =
      underlying.getOrElse(k, otherwise).asInstanceOf[B]

    def get[B <: A](k: B): Option[B] =
      underlying.get(k).asInstanceOf[Option[B]]

    override def toString =
      underlying.iterator.map { case (a, b) => s"$a -> $b" }.mkString("DMap(", ", ", ")")
  }

  private object DMap {
    def empty[A] = new DMap[A]
  }

  class View[MT <: MetaTypes] private[IsomorphismState](
    forwardTables: Map[AutoTableLabel, AutoTableLabel],
    backwardTables: Map[AutoTableLabel, AutoTableLabel],
    forwardColumns: DMap[(Option[AutoTableLabel], types.ColumnLabel[MT])],
    backwardColumns: DMap[(Option[AutoTableLabel], types.ColumnLabel[MT])],
    forwardCTEs: Map[AutoCTELabel, AutoCTELabel],
    backwardCTEs: Map[AutoCTELabel, AutoCTELabel]
  ) extends LabelUniverse[MT] {
    def reverse: View[MT] =
      new View(backwardTables, forwardTables, backwardColumns, forwardColumns, backwardCTEs, forwardCTEs)

    private[analyzer2] def renameForward(t: AutoTableLabel): AutoTableLabel = forwardTables.getOrElse(t, t)
    private[analyzer2] def renameBackward(t: AutoTableLabel): AutoTableLabel = backwardTables.getOrElse(t, t)

    private[analyzer2] def renameForward[C <: ColumnLabel](t: Option[AutoTableLabel], c: C): (Option[AutoTableLabel], C) =
      forwardColumns.getOrElse((t, c), (t, c))
    private[analyzer2] def renameBackward[C <: ColumnLabel](t: Option[AutoTableLabel], c: C): (Option[AutoTableLabel], C) =
      backwardColumns.getOrElse((t, c), (t, c))

    private[analyzer2] def extend: IsomorphismState =
      new IsomorphismState(
        forwardTables, backwardTables,
        forwardColumns, backwardColumns,
        forwardCTEs, backwardCTEs
      )

    override def toString =
      s"IsomorphismState(\n  $forwardTables,\n  $backwardTables,\n  $forwardColumns, $backwardColumns\n,  $forwardCTEs, $backwardCTEs\n)"
  }

  object View {
    def empty[MT <: MetaTypes] =
      new View[MT](Map.empty, Map.empty, DMap.empty, DMap.empty, Map.empty, Map.empty)
  }
}

class IsomorphismState[MT <: MetaTypes] private (
  private var forwardTables: Map[types.AutoTableLabel[MT], types.AutoTableLabel[MT]],
  private var backwardTables: Map[types.AutoTableLabel[MT], types.AutoTableLabel[MT]],
  private var forwardColumns: IsomorphismState.DMap[(Option[types.AutoTableLabel[MT]], types.ColumnLabel[MT])],
  private var backwardColumns: IsomorphismState.DMap[(Option[types.AutoTableLabel[MT]], types.ColumnLabel[MT])],
  private var forwardCTEs: Map[types.AutoCTELabel[MT], types.AutoCTELabel[MT]],
  private var backwardCTEs: Map[types.AutoCTELabel[MT], types.AutoCTELabel[MT]]
) extends LabelUniverse[MT] {
  private[analyzer2] def this() = this(Map.empty, Map.empty, new IsomorphismState.DMap, new IsomorphismState.DMap, Map.empty, Map.empty)

  private [analyzer2] def attempt(f: IsomorphismState => Boolean): Boolean = {
    val clone = new IsomorphismState(forwardTables, backwardTables, forwardColumns, backwardColumns, forwardCTEs, backwardCTEs)
    val result = f(clone)
    if(result) {
      this.forwardTables = clone.forwardTables
      this.backwardTables = clone.backwardTables
      this.forwardColumns = clone.forwardColumns
      this.backwardColumns = clone.backwardColumns
      this.forwardCTEs = clone.forwardCTEs
      this.backwardCTEs = clone.backwardCTEs
    }
    result
  }

  def finish = new IsomorphismState.View(forwardTables, backwardTables, forwardColumns, backwardColumns, forwardCTEs, backwardCTEs)

  def mapFrom(table: Option[AutoTableLabel], col: ColumnLabel): Option[(Option[AutoTableLabel], ColumnLabel)] =
    forwardColumns.get((table, col))

  def alreadyAssociated(cteA: AutoCTELabel, cteB: AutoCTELabel): Boolean =
    forwardCTEs.get(cteA) == Some(cteB)

  def tryAssociate(tableA: AutoTableLabel, tableB: AutoTableLabel): Boolean =
    tryIntern(tableA, tableB)

  def tryAssociate(cteA: AutoCTELabel, cteB: AutoCTELabel): Boolean =
    tryIntern(cteA, cteB)

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

  private def tryIntern(tableA: AutoTableLabel, tableB: AutoTableLabel): Boolean = {
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

  private def tryIntern(tableA: Option[AutoTableLabel], tableB: Option[AutoTableLabel]): Boolean = {
    (tableA, tableB) match {
      case (Some(ta), Some(tb)) => tryIntern(ta, tb)
      case (None, None) => true
      case _ => false
    }
  }

  private def tryIntern[C <: ColumnLabel](tableA: Option[AutoTableLabel], columnA: C, tableB: Option[AutoTableLabel], columnB: C): Boolean = {
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

  private def tryIntern(cteA: AutoCTELabel, cteB: AutoCTELabel): Boolean = {
    (forwardCTEs.get(cteA), backwardCTEs.get(cteB)) match {
      case (None, None) =>
        forwardCTEs += cteA -> cteB
        backwardCTEs += cteB -> cteA
        true
      case (Some(b), Some(a)) =>
        a == cteA && b == cteB
      case _ =>
        false
    }
  }
}
