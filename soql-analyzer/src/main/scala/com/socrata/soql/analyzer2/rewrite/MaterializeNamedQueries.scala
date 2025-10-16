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
    private val queries = new mutable.LinkedHashMap[CanonicalName, Vector[CTEStuff]]

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
      val cteDefs = OrderedMap() ++ NamedQueries.things.map { case CTEStuff(label, defQuery) =>
        label -> CTE.Definition(None, defQuery, MaterializedHint.Default)
      }
      val newStmt = rewriteStatement(stmt)
      CTE(cteDefs, newStmt)
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
      case stmt@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        val (columnMap, newFrom) = from.reduceMap[ColumnMap, MT](
          rewriteAtomicFrom,
          { (rewrote, jt, lat, left, right, on) =>
            val (rewroteRight, newRight) = rewriteAtomicFrom(right)
            (rewrote ++ rewroteRight, Join(jt, lat, left, newRight, on))
          }
        )
        val stmtWithPossiblyDanglingColumnRefs = stmt.copy(from = newFrom)
        if(columnMap.nonEmpty) {
          rewriteExprs(columnMap, stmtWithPossiblyDanglingColumnRefs)
        } else { // no dangling column refs
          stmtWithPossiblyDanglingColumnRefs
        }
    }

  private def rewriteExprs(cm: ColumnMap, stmt: Statement): Statement =
    stmt match {
      case CombinedTables(op, left, right) =>
        CombinedTables(op, rewriteExprs(cm, left), rewriteExprs(cm, right))
      case _ : CTE =>
        throw new Exception("Shouldn't see CTEs at this point")
      case Values(labels, values) =>
        Values(labels, values.map(_.map(rewriteExprs(cm, _))))
      case stmt@Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
        Select(
          rewriteExprs(cm, distinctiveness),
          rewriteExprs(cm, selectList),
          rewriteExprs(cm, from),
          where.map(rewriteExprs(cm, _)),
          groupBy.map(rewriteExprs(cm, _)),
          having.map(rewriteExprs(cm, _)),
          orderBy.map(rewriteExprs(cm, _)),
          limit,
          offset,
          search,
          hint
        )
    }

  private def rewriteExprs(cm: ColumnMap, d: Distinctiveness): Distinctiveness =
    d match {
      case Distinctiveness.On(exprs) => Distinctiveness.On(exprs.map(rewriteExprs(cm, _)))
      case other@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) => other
    }

  private def rewriteExprs(cm: ColumnMap, selectList: OrderedMap[AutoColumnLabel, NamedExpr]): OrderedMap[AutoColumnLabel, NamedExpr] =
    OrderedMap() ++ selectList.iterator.map { case (colLabel, namedExpr) => colLabel -> namedExpr.copy(expr = rewriteExprs(cm, namedExpr.expr)) }

  private def rewriteExprs(cm: ColumnMap, e: Expr): Expr = {
    e match {
      case vc@VirtualColumn(t, c, typ) =>
        cm.get(t) match {
          case Some(cols) => VirtualColumn(t, cols(c), typ)(vc.position)
          case None => vc
        }
      case otherAtomic: AtomicExpr =>
        otherAtomic
      case fc@FunctionCall(func, args) =>
        FunctionCall(func, args.map(rewriteExprs(cm, _)))(fc.position)
      case afc@AggregateFunctionCall(func, args, distinct, filter) =>
        AggregateFunctionCall(func, args.map(rewriteExprs(cm, _)), distinct, filter.map(rewriteExprs(cm, _)))(afc.position)
      case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) =>
        WindowedFunctionCall(func, args.map(rewriteExprs(cm, _)), filter.map(rewriteExprs(cm, _)), partitionBy.map(rewriteExprs(cm, _)), orderBy.map(rewriteExprs(cm, _)), frame)(wfc.position)
    }
  }

  private def rewriteExprs(cm: ColumnMap, f: From): From = {
    f.map[MT](
      rewriteAFExprs(cm, _),
      { (joinType, lateral, left, right, on) =>
        val newRight = rewriteAFExprs(cm, right)
        Join(joinType, lateral, left, newRight, rewriteExprs(cm, on))
      }
    )
  }

  private def rewriteExprs(cm: ColumnMap, ob: OrderBy): OrderBy =
    ob.copy(expr = rewriteExprs(cm, ob.expr))

  // This is annoying, but required to support lateral joins
  private def rewriteAFExprs(cm: ColumnMap, f: AtomicFrom): AtomicFrom = {
    f match {
      case fs: FromStatement => fs.copy(statement = rewriteExprs(cm, fs.statement))
      case ft: FromTable => ft
      case fsr: FromSingleRow => fsr
      case fc: FromCTE => fc.copy(basedOn = rewriteExprs(cm, fc.basedOn))
    }
  }

  private def rewriteAtomicFrom(f: AtomicFrom): (ColumnMap, AtomicFrom) =
    f match {
      case orig@FromStatement(origStmt, label, Some(rn), Some(cn), alias) => // "Some(rn)" means this is a saved query
        val rewritten = rewriteStatement(origStmt)
        NamedQueries.retrieveCached(cn, rewritten) match {
          case Some(ctestuff) if ctestuff.reused =>
            // ctestuff.defQuery is isomorphic to stmt, so we can line
            // up the output columns...
            assert(rewritten.schema.size == ctestuff.defQuery.schema.size)
            val mappedColumns = rewritten.schema.iterator.zip(ctestuff.defQuery.schema.iterator).map {
              case ((myColLabel, mySchemaEntry), (theirColLabel, theirSchemaEntry)) =>
                assert(mySchemaEntry.typ == theirSchemaEntry.typ)
                myColLabel -> theirColLabel
            }.toMap
            (Map(label -> mappedColumns), FromCTE(ctestuff.label, label, rewritten, mappedColumns, rn, cn, alias))
          case _ =>
            // can't replace this whole
            (Map.empty, orig.copy(statement = rewriteStatement(rewritten)))
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
