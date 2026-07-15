package com.socrata.soql.analyzer2.rewrite

import scala.collection.mutable

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._

class MaterializeQueries[MT <: MetaTypes] private (labelProvider: LabelProvider) extends StatementUniverse[MT] {
  // The tricky bit is identifying when two things name the "same"
  // query, because while if two ScopedResourceNames are the same then
  // they're definitely the same thing, but if they're different they
  // aren't necessarily.  Perhaps we should use structural equality
  // rather than relying on names?

  private case class CTEStuff(label: AutoCTELabel, defQuery: Statement) {
    // This is sort of gnarly flow, but, this works in two passes.
    // First, we collect all the named queries (in NamedQueries), then
    // we rewrite them using that same cache.  This lazy val lets us
    // get the single canonical copy of the CTE statement with
    // rewrites applied; it's only accessed after we're done
    // populating NamedQueries.
    lazy val rewrittenDefQuery = rewriteStatement(defQuery)
  }

  private object ObservedQueries {
    // The "same" canonical name can name multiple different queries,
    // because the actual query you get can (theoretically) differ
    // based on who you are.
    private val queries = new mutable.ArrayBuffer[CTEStuff]

    // We only store UN-REWRITTEN queries in the cache!
    def retrieveCached(s: Statement): Option[CTEStuff] =
      queries.find(_.defQuery.isIsomorphic(s))

    def save(s: CTEStuff): Unit = {
      queries += s
    }

    def any = queries.nonEmpty
    def things: Iterator[CTEStuff] = queries.iterator
  }

  def rewriteTopLevelStatement(stmt: Statement): Statement = {
    collectObservedQueries(stmt)

    if(ObservedQueries.any) {
      val cteDefs = OrderedMap() ++ ObservedQueries.things.map { case ctestuff =>
        ctestuff.label -> CTE.Definition(None, ctestuff.rewrittenDefQuery, MaterializedHint.Materialized)
      }
      val newStmt = rewriteStatement(stmt)
      CTE(cteDefs, newStmt)
    } else {
      stmt
    }
  }

  def markedAsMaterializable(stmt: Statement): Boolean =
    stmt match {
      case s: Select =>
        s.hint(SelectHint.Materialized)
      case _ =>
        false
    }

  private def collectObservedQueries(stmt: Statement): Unit = {
    stmt match {
      case CombinedTables(op, left, right) =>
        collectObservedQueries(left)
        collectObservedQueries(right)
      case _ : CTE =>
        throw new Exception("Shouldn't see CTEs at this point")
      case v: Values =>
        // no subqueries in values
      case sel: Select =>
        sel.from.reduce[Unit](
          collectAtomicFromObservedQueries,
          (_, join) => collectAtomicFromObservedQueries(join.right)
        )
    }
  }

  private def collectAtomicFromObservedQueries(f: AtomicFrom): Unit = {
    f match {
      case FromStatement(stmt, _, _, _, _) if markedAsMaterializable(stmt) && !stmt.containsNonlocalColumnReferences =>
        ObservedQueries.retrieveCached(stmt) match {
          case None =>
            collectObservedQueries(stmt)
            ObservedQueries.save(CTEStuff(labelProvider.cteLabel(), stmt))
          case Some(ctestuff) =>
            // ok
          }
      case FromStatement(stmt, _, _, _, _) =>
        collectObservedQueries(stmt)
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
    selectList.withValuesMapped { case namedExpr => namedExpr.copy(expr = rewriteExprs(cm, namedExpr.expr)) }

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
      case fc: FromCTE => fc // don't need to rewrite anything because we won't have created it if it had references that needed rewriting
    }
  }

  private def rewriteAtomicFrom(f: AtomicFrom): (ColumnMap, AtomicFrom) =
    f match {
      case orig@FromStatement(stmt, label, rn, cn, alias) if markedAsMaterializable(stmt) && !stmt.containsNonlocalColumnReferences =>
        ObservedQueries.retrieveCached(stmt) match {
          case Some(ctestuff) =>
            // ctestuff.defQuery is isomorphic to stmt, so we can line
            // up the output columns...
            assert(stmt.schema.size == ctestuff.defQuery.schema.size)
            val mappedColumns = stmt.schema.iterator.zip(ctestuff.defQuery.schema.iterator).flatMap {
              case ((myColLabel, mySchemaEntry), (theirColLabel, theirSchemaEntry)) =>
                assert(mySchemaEntry.typ == theirSchemaEntry.typ)
                if(myColLabel == theirColLabel) {
                  None
                } else {
                  Some(myColLabel -> theirColLabel)
                }
            }.toMap

            // We'll only need to rewrite exprs if we're using
            // "someone else's" Statement to define the CTE.
            val columnMap: ColumnMap = if(mappedColumns.isEmpty) Map.empty else Map(label -> mappedColumns)

            (columnMap, FromCTE(ctestuff.label, label, ctestuff.rewrittenDefQuery, rn, cn, alias))
          case _ =>
            // This shouldn't happen... we've checked that it is
            // marked as materializable and doesn't contain external
            // references, so it _should_ be cached.  But since it's
            // not, just recursively rewrite the query to catch any
            // interior reuse.
            (Map.empty, orig.copy(statement = rewriteStatement(stmt)))
        }
      case orig@FromStatement(stmt, _, _, _, _) =>
        (Map.empty, orig.copy(statement = rewriteStatement(stmt)))
      case other =>
        (Map.empty, other)
    }
}

object MaterializeQueries {
  @volatile var validationActive = false

  def apply[MT <: MetaTypes](labelProvider: LabelProvider, statement: Statement[MT]): Statement[MT] = {
    val result = new MaterializeQueries[MT](labelProvider).rewriteTopLevelStatement(statement)
    assert(result.referencedCTEs == statement.referencedCTEs)
    result
  }
}
