package com.socrata.soql.analyzer2.rewrite

import scala.collection.mutable

import com.socrata.soql.collection.OrderedMap
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

  private case class CTEStuff(label: AutoTableLabel, defQuery: Statement) {
    var reused = false
  }

  private object NamedQueries {
    // The "same" canonical name can name multiple different queries,
    // because the actual query you get can (theoretically) differ
    // based on who you are.
    private val queries = new mutable.HashMap[CanonicalName, Vector[CTEStuff]]

    def retrieveCached(x: CanonicalName, s: Statement): Option[CTEStuff] =
      queries.getOrElse(x, Vector.empty).find(_.defQuery.isIsomorphic(s))

    def save(x: CanonicalName, s: CTEStuff): Unit = {
      queries.get(x) match {
        case None => queries += x -> Vector(s)
        case Some(v) => queries += x -> (v :+ s)
      }
    }

    def any = queries.valuesIterator.flatMap(_.iterator).exists(_.reused)
    def things: Iterator[CTEStuff] = queries.valuesIterator.flatMap(_.iterator).filter(_.reused)
  }

  def rewriteTopLevelStatement(stmt: Statement): Statement = {
    collectNamedQueries(stmt)

    if(NamedQueries.any) {
      val queries = NamedQueries.things.toVector.sortBy(_.label)
      queries.foldLeft(rewriteStatement(stmt)) { (stmt, ctestuff) =>
        CTE(ctestuff.label, None, ctestuff.defQuery, MaterializedHint.Default, stmt)
      }
    } else {
      stmt
    }
  }

  private def collectNamedQueries(stmt: Statement): Unit = {
    stmt match {
      case CombinedTables(op, left, right) =>
        collectNamedQueries(left)
        collectNamedQueries(right)
      case _ : CTE =>
        throw new Exception("Shouldn't see CTEs at this point")
      case v: Values =>
        // no subqueries in values
      case sel: Select =>
        sel.from.reduce[Unit](
          collectAtomicFromNamedQueries,
          (_, join) => collectAtomicFromNamedQueries(join.right)
        )
    }
  }

  private def collectAtomicFromNamedQueries(f: AtomicFrom): Unit = {
    f match {
      case FromStatement(stmt, _, _, Some(cn), _) =>
        NamedQueries.retrieveCached(cn, stmt) match {
          case None =>
            NamedQueries.save(cn, CTEStuff(labelProvider.tableLabel(), stmt))
          case Some(ctestuff) =>
            ctestuff.reused = true
        }
      case _ =>
        ()
    }
  }

  // will need to rewrite VirtualColumns to refer to the output columns of the CTE
  private type ColumnMap = Map[AutoTableLabel, Map[AutoColumnLabel, AutoColumnLabel]]

  private def rewriteStatement(stmt: Statement): Statement =
    stmt match {
      case CombinedTables(op, left, right) =>
        CombinedTables(op, rewriteStatement(left), rewriteStatement(right))
      case _ : CTE =>
        throw new Exception("Shouldn't see CTEs at this point")
      case v: Values =>
        v
      case Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val (columnMap, newFrom) = from.reduceMap[ColumnMap, MT](
          rewriteAtomicFrom,
          { (rewrote, jt, lat, left, right, on) =>
            val (rewroteRight, newRight) = rewriteAtomicFrom(right)
            (rewrote ++ rewroteRight, Join(jt, lat, left, newRight, on))
          }
        )
        Select(
          rewriteExpr(columnMap, distinctiveness),
          rewriteExpr(columnMap, selectList),
          rewriteExpr(columnMap, newFrom),
          where.map(rewriteExpr(columnMap, _)),
          groupBy.map(rewriteExpr(columnMap, _)),
          having.map(rewriteExpr(columnMap, _)),
          orderBy.map(rewriteExpr(columnMap, _)),
          limit,
          offset,
          search,
          hint
        )
    }

  private def rewriteExpr(cm: ColumnMap, d: Distinctiveness): Distinctiveness =
    d match {
      case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(rewriteExpr(cm, _)))
      case other@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) => other
    }

  private def rewriteExpr(cm: ColumnMap, selectList: OrderedMap[AutoColumnLabel, NamedExpr]): OrderedMap[AutoColumnLabel, NamedExpr] =
    OrderedMap() ++ selectList.iterator.map { case (colLabel, namedExpr) => colLabel -> namedExpr.copy(expr = rewriteExpr(cm, namedExpr.expr)) }

  private def rewriteExpr(cm: ColumnMap, e: Expr): Expr = {
    e match {
      case vc@VirtualColumn(t, c, typ) =>
        cm.get(t) match {
          case Some(cols) => VirtualColumn(t, cols(c), typ)(vc.position)
          case None => vc
        }
      case otherAtomic: AtomicExpr =>
        otherAtomic
      case fc@FunctionCall(func, args) =>
        FunctionCall(func, args.map(rewriteExpr(cm, _)))(fc.position)
      case afc@AggregateFunctionCall(func, args, distinct, filter) =>
        AggregateFunctionCall(func, args.map(rewriteExpr(cm, _)), distinct, filter.map(rewriteExpr(cm, _)))(afc.position)
      case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) =>
        WindowedFunctionCall(func, args.map(rewriteExpr(cm, _)), filter.map(rewriteExpr(cm, _)), partitionBy.map(rewriteExpr(cm, _)), orderBy.map(rewriteExpr(cm, _)), frame)(wfc.position)
    }
  }

  private def rewriteExpr(cm: ColumnMap, f: From): From = {
    f.reduceMap[Boolean, MT](
      { leftmost => (false, leftmost) },
      { (seenLateral, joinType, lateral, left, right, on) =>
        val anyLateral = seenLateral || lateral
        val newRight = if(anyLateral) rewriteAFExpr(cm, right) else right
        (anyLateral, Join(joinType, lateral, left, newRight, rewriteExpr(cm, on)))
      }
    )._2
  }

  private def rewriteExpr(cm: ColumnMap, ob: OrderBy): OrderBy =
    ob.copy(expr = rewriteExpr(cm, ob.expr))

  // This is annoying, but required to support lateral joins
  private def rewriteAFExpr(cm: ColumnMap, f: AtomicFrom): AtomicFrom = {
    ???
  }

  private def rewriteAtomicFrom(f: AtomicFrom): (ColumnMap, AtomicFrom) =
    f match {
      case orig@FromStatement(stmt, label, Some(rn), Some(cn), alias) => // "Some(rn)" means this is a saved query
        NamedQueries.retrieveCached(cn, stmt) match {
          case Some(ctestuff) if ctestuff.reused =>
            // ctestuff.defQuery is isomorphic to stmt, so we can line
            // up the output columns...
            assert(stmt.schema.size == ctestuff.defQuery.schema.size)
            val mappedColumns = stmt.schema.iterator.zip(ctestuff.defQuery.schema.iterator).map {
              case ((myColLabel, mySchemaEntry), (theirColLabel, theirSchemaEntry)) =>
                assert(mySchemaEntry.typ == theirSchemaEntry.typ)
                myColLabel -> theirColLabel
            }.toMap
            (Map(label -> mappedColumns), FromCTE(ctestuff.label, label, stmt, mappedColumns, rn, cn, alias))
          case _ =>
            // can't replace this whole
            (Map.empty, orig.copy(statement = rewriteStatement(stmt)))
        }
      case other =>
        (Map.empty, other)
    }
}

object MaterializeNamedQueries {
  def apply[MT <: MetaTypes](labelProvider: LabelProvider, statement: Statement[MT]): Statement[MT] = {
    new MaterializeNamedQueries[MT](labelProvider).rewriteTopLevelStatement(statement)
  }
}
