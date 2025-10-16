package com.socrata.soql.sqlizer

import scala.collection.{mutable => scm}
import scala.collection.compat._
import scala.{collection => sc}

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance

class ProvenanceTracker[MT <: MetaTypes with MetaTypesExt] private (
  exprs: sc.Map[Expr[MT], ProvenanceTracker.Result]
) extends SqlizerUniverse[MT] {
  // Returns the set of possible origin-tables for the value of this
  // expression.  In a tree where this is size <2 but demanded by an
  // expression with size >=2 is where provenances should start
  // getting tracked dynamically.
  def apply(expr: Expr): ProvenanceTracker.Result =
    exprs(expr)
}

object ProvenanceTracker {
  sealed abstract class Result {
    def isPlural: Boolean
    def ++(that: Result): Result
  }
  object Result {
    val empty: Result = Known(Set.empty)
  }
  case class Known(possibilities: Set[Option[Provenance]]) extends Result {
    val isPlural = possibilities.size > 1
    def ++(that: Result) =
      that match {
        case Known(otherPossibilities) => Known(possibilities ++ otherPossibilities)
        case Unknown => Unknown
      }
  }
  case object Unknown extends Result {
    def isPlural = true
    def ++(that: Result) = this
  }

  def apply[MT <: MetaTypes with MetaTypesExt](
    s: Statement[MT],
    provenanceOf: LiteralValue[MT] => Set[Option[Provenance]],
    dtnToProvenance: types.ToProvenance[MT],
    isRollup: types.DatabaseTableName[MT] => Boolean
  ): ProvenanceTracker[MT] = {
    val builder = new Builder[MT](provenanceOf, dtnToProvenance, isRollup)
    builder.processStatement(s)
    new ProvenanceTracker(builder.exprs)
  }

  private class Builder[MT <: MetaTypes with MetaTypesExt](
    provenanceOf: LiteralValue[MT] => Set[Option[Provenance]],
    dtnToProvenance: types.ToProvenance[MT],
    isRollup: types.DatabaseTableName[MT] => Boolean
  ) extends SqlizerUniverse[MT] {
    val exprs = new scm.HashMap[Expr, Result]
    val identifiers = new scm.HashMap[(AutoTableLabel, ColumnLabel), Result]

    private val emptySLR = immutable.ArraySeq[Result]()

    def processStatement(s: Statement): Seq[Result] = {
      s match {
        case CombinedTables(op, left, right) =>
          processStatement(left).lazyZip(processStatement(right)).map(_ ++ _)
        case CTE(defns, useQuery) =>
          for(defn <- defns.valuesIterator) {
            processStatement(defn.query)
          }
          processStatement(useQuery)

        case Values(labels, values) =>
          val result = Array.fill(labels.size)(Result.empty)
          for(row <- values) {
            for((col, i) <- row.iterator.zipWithIndex) {
              val prov = processExpr(col, emptySLR)
              result(i) ++= prov
            }
          }
          immutable.ArraySeq.unsafeWrapArray(result) // safety: nothing else has a reference to "result"

        case Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
          // Doing From first to populate identifiers
          processFrom(from)

          // Then selectList so that provs can be attached to selectListProvenances
          val selectListProvenances = locally {
            val result = new Array[Result](selectList.size)
            for((namedExpr, idx) <- selectList.valuesIterator.zipWithIndex) {
              result(idx) = processExpr(namedExpr.expr, emptySLR)
            }

            immutable.ArraySeq.unsafeWrapArray(result) // safety: nothing else has a reference to "result"
          }

          // now all the rest
          distinctiveness match {
            case Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct() =>
              // nothing to do
            case Distinctiveness.On(exprs) =>
              for(e <- exprs) {
                processExpr(e, selectListProvenances)
              }
          }

          for(w <- where) {
            processExpr(w, emptySLR) // where can't use selectlistferences
          }

          for(gb <- groupBy) {
            processExpr(gb, selectListProvenances)
          }

          for(h <- having) {
            processExpr(h, emptySLR) // having can't use selectlistferences
          }

          for(ob <- orderBy) {
            processExpr(ob.expr, selectListProvenances)
          }

          selectListProvenances
      }
    }

    def processFrom(f: From): Unit = {
      f.reduce[Unit](
        processAtomicFrom(_),
        { case ((), join) =>
          processAtomicFrom(join.right)
          processExpr(join.on, emptySLR)
        }
      )
    }

    def processAtomicFrom(af: AtomicFrom): Unit = {
      af match {
        case _ : FromSingleRow =>
          // no structure here
        case ft: FromTable =>
          val prov =
            if(isRollup(ft.tableName)) {
              Unknown
            } else {
              Known(Set(Some(dtnToProvenance.toProvenance(ft.tableName))))
            }
          for(col <- ft.columns.keysIterator) {
            identifiers += (ft.label, col) -> prov
          }
        case fs: FromStatement =>
          processStatement(fs.statement).lazyZip(fs.schema).foreach { (prov, schemaInfo) =>
            identifiers += (schemaInfo.table, schemaInfo.column) -> prov
          }
        case fc : FromCTE =>
          processStatement(fc.basedOn).lazyZip(fc.schema).foreach { (prov, schemaInfo) =>
            identifiers += (schemaInfo.table, schemaInfo.column) -> prov
          }
      }
    }

    def processExpr(e: Expr, selectList: immutable.ArraySeq[Result]): Result = {
      val prov: Result =
        e match {
          case l: LiteralValue =>
            Known(provenanceOf(l))
          case c: Column =>
            identifiers((c.table, c.column))
          case n: NullLiteral =>
            Result.empty
          case FunctionCall(_func, args) =>
            args.foldLeft(Result.empty) { (acc, arg) =>
              acc ++ processExpr(arg, emptySLR) // selectlistreferences must be top-level
            }
          case AggregateFunctionCall(_func, args, _distinct, filter) =>
            val argProvs =
              args.foldLeft(Result.empty) { (acc, arg) =>
                acc ++ processExpr(arg, emptySLR)
              }
            filter.foldLeft(argProvs) { (acc, filter) =>
              acc ++ processExpr(filter, emptySLR)
            }
          case WindowedFunctionCall(_func, args, filter, partitionBy, orderBy, _frame) =>
            val argProv =
              args.foldLeft(Result.empty) { (acc, arg) =>
                acc ++ processExpr(arg, emptySLR)
              }
            val filterProv =
              filter.foldLeft(argProv) { (acc, filter) =>
                acc ++ processExpr(filter, emptySLR)
              }
            val partitionByProv =
              partitionBy.foldLeft(filterProv) { (acc, partitionBy) =>
                acc ++ processExpr(partitionBy, emptySLR)
              }
            orderBy.foldLeft(partitionByProv) { (acc, ob) =>
              acc ++ processExpr(ob.expr, emptySLR)
            }
          case SelectListReference(i, _, _, _) =>
            selectList(i - 1)
        }

      exprs(e) = prov

      prov
    }
  }
}
