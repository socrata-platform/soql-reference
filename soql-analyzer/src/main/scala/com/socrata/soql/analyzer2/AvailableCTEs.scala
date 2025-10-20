package com.socrata.soql.analyzer2

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

  def rebaseAll(f: From[MT]): From[MT] =
    new AvailableCTEs.Rebaser[MT, T](ctes).rebaseAll(f)

  def rebaseAllAtomic(f: AtomicFrom[MT]): AtomicFrom[MT] =
    new AvailableCTEs.Rebaser[MT, T](ctes).rebaseAllAtomic(f)

  def rebaseAll(s: Statement[MT]): Statement[MT] =
    new AvailableCTEs.Rebaser[MT, T](ctes).rebaseAll(s)

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

  private class Rebaser[MT <: MetaTypes, +T](ctes: Map[AutoTableLabel, AvailableCTE[MT, T]]) {
    private val justTheStatements = ctes.iterator.map { case (k, v) => k -> v.stmt }.toMap

    def rebaseAll(f: From[MT]): From[MT] =
      if(justTheStatements.isEmpty) f
      else rebaseAll(justTheStatements, f)

    private def rebaseAll(ctes: Map[AutoTableLabel, Statement[MT]], f: From[MT]): From[MT] =
      f.map[MT](
        rebaseAllAtomic(ctes, _),
        { (joinType, lat, left, right, on) => Join(joinType, lat, left, rebaseAllAtomic(ctes, right), on) }
      )

    def rebaseAllAtomic(f: AtomicFrom[MT]): AtomicFrom[MT] =
      if(justTheStatements.isEmpty) f
      else rebaseAllAtomic(justTheStatements, f)

    private def rebaseAllAtomic(ctes: Map[AutoTableLabel, Statement[MT]], f: AtomicFrom[MT]): AtomicFrom[MT] = {
      f match {
        case fs: FromStatement[MT] => fs.copy(statement = rebaseAll(ctes, fs.statement))
        case fc: FromCTE[MT] => fc.copy(basedOn = ctes(fc.cteLabel))
        case other => other
      }
    }

    def rebaseAll(s: Statement[MT]): Statement[MT] =
      if(justTheStatements.isEmpty) s
      else rebaseAll(justTheStatements, s)

    private def rebaseAll(ctes: Map[AutoTableLabel, Statement[MT]], s: Statement[MT]): Statement[MT] = {
      s match {
        case CombinedTables(op, left, right) =>
          CombinedTables(op, rebaseAll(ctes, left), rebaseAll(ctes, right))
        case v: Values[MT] =>
          v
        case CTE(defns, useQuery) =>
          val (newCTEs, newDefns) = defns.iterator.foldLeft((ctes, OrderedMap.empty[AutoTableLabel, CTE.Definition[MT]])) { case ((ctes, newDefns), (label, defn)) =>
            val newStmt = rebaseAll(ctes, defn.query)
            (ctes + (label -> newStmt), newDefns + (label -> defn.copy(query = newStmt)))
          }
          val newUseQuery = rebaseAll(newCTEs, useQuery)
          CTE(newDefns, newUseQuery)
        case sel: Select[MT] =>
          sel.copy(from = rebaseAll(ctes, sel.from))
      }
    }
  }
}
