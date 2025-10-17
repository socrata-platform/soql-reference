package com.socrata.soql.analyzer2.rewrite

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

case class AvailableCTE[MT <: MetaTypes, +T](
  stmt: Statement[MT],
  extra: T,
)
case class AvailableCTEs[MT <: MetaTypes, T](
  ctes: Map[AutoTableLabel, AvailableCTE[MT, T]]
) {
  def add[U >: T](
    label: AutoTableLabel,
    stmt: Statement[MT],
    extra: U
  ): AvailableCTEs[MT, U] = {
    assert(!ctes.contains(label))
    copy(ctes = ctes + (label -> AvailableCTE(stmt, extra)))
  }

  def rebase(fc: FromCTE[MT]) =
    fc.copy(basedOn = ctes(fc.cteLabel).stmt)

  def collect(
    defns: OrderedMap[AutoTableLabel, CTE.Definition[MT]]
  )(
    f: (AvailableCTEs[MT, T], Statement[MT]) => (T, Statement[MT])
  ): (AvailableCTEs[MT, T], OrderedMap[AutoTableLabel, CTE.Definition[MT]]) = {
    defns.iterator.foldLeft((this, OrderedMap.empty[AutoTableLabel, CTE.Definition[MT]])) { case ((aCTE, newDefns), (label, defn)) =>
      val (extra, newStmt) = f(aCTE, defn.query)
      (aCTE.add(label, newStmt, extra), newDefns + (label -> defn.copy(query = newStmt)))
    }
  }

  def foldCollect[B](
    defns: OrderedMap[AutoTableLabel, CTE.Definition[MT]],
    init: B
  )(
    f: (B, AvailableCTEs[MT, T], Statement[MT]) => (B, T, Statement[MT])
  ): (B, AvailableCTEs[MT, T], OrderedMap[AutoTableLabel, CTE.Definition[MT]]) = {
    defns.iterator.foldLeft((init, this, OrderedMap.empty[AutoTableLabel, CTE.Definition[MT]])) { case ((state, aCTE, newDefns), (label, defn)) =>
      val (newState, extra, newStmt) = f(state, aCTE, defn.query)
      (newState, aCTE.add(label, newStmt, extra), newDefns + (label -> defn.copy(query = newStmt)))
    }
  }
}
object AvailableCTEs {
  def empty[MT <: MetaTypes, T] = AvailableCTEs[MT, T](Map.empty)
}
