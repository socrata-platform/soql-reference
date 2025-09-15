package com.socrata.soql.analyzer2.rewrite

import scala.collection.mutable

import com.socrata.soql.analyzer2._

class MaterializeNamedQueries[MT <: MetaTypes] private (labelProvider: LabelProvider) extends StatementUniverse[MT] {
  // ok so, we want to walk over the Statement, and for each select,
  // if a subquery in its FROM has a resource name we want to hoist it
  // to the top level as a CTE and replace references to it.
  //
  // Despite the name of the pass, right now we're _not_ explicitly
  // materializing the query.  Instead we're relying on PG's default
  // heuristic (i.e., "if it's referenced more than once, materialize,
  // otherwise don't")
  //
  // The tricky bit is identifying when two things name the "same"
  // query, because while if two ScopedResourceNames are the same then
  // they're definitely the same thing, but if they're different they
  // aren't necessarily.  Perhaps we should use structural equality
  // rather than relying on names?

  private case class CTEStuff(label: AutoTableLabel, defQuery: Statement)

  private val ctes = new mutable.LinkedHashMap[ScopedResourceName, CTEStuff]()

  def rewriteTopLevelStatement(stmt: Statement): Statement =
    ctes.toVector.foldRight(rewriteStatement(stmt)) { case ((srn, CTEStuff(label, defQuery)), rewritten) =>
      CTE(label, None, defQuery, MaterializedHint.Default, rewritten)
    }

  private def rewriteStatement(stmt: Statement): Statement =
    stmt match {
      case CombinedTables(op, left, right) =>
        CombinedTables(op, rewriteStatement(left), rewriteStatement(right))
      case _ : CTE =>
        throw new Exception("Shouldn't see CTEs at this point")
      case v: Values =>
        v
      case sel: Select =>
        val newFrom = sel.from.map[MT](
          rewriteAtomicFrom,
          (jt, lat, left, right, on) => Join(jt, lat, left, rewriteAtomicFrom(right), on)
        )
        sel.copy(from = newFrom)
    }

  private def rewriteAtomicFrom(f: AtomicFrom): AtomicFrom =
    f match {
      case FromStatement(stmt, label, Some(rn), alias) => // "Some(rn)" means this is a saved query
        val rewritten = rewriteStatement(stmt)
        // ok, so now what?  Presumably we need to have some kind of
        // new FromCTE 
        FromStatement(rewritten, label, Some(rn), alias)
      case other =>
        other
    }
}

object MaterializeNamedQueries {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, statement: Statement[MT]): Statement[MT] = {
    new MaterializeNamedQueries[MT](labelProvider).rewriteTopLevelStatement(statement)
  }
}
