package com.socrata.soql.sqlizer

import scala.collection.compat.immutable.LazyList

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.MonomorphicFunction

abstract class RewriteSearch[MT <: MetaTypes with MetaTypesExt]
    extends StatementUniverse[MT]
{
  val searchBeforeQuery: Boolean

  def apply(stmt: Statement): Statement = rewriteStatement(stmt)

  // Produce a term that can be used to create an index on a table with the given schema
  def searchTerm(schema: Iterable[(ColumnLabel, Rep[MT])]): Option[Doc[Nothing]]

  private def rewriteStatement(stmt: Statement): Statement =
    stmt match {
      case CombinedTables(op, left, right) =>
        CombinedTables(op, rewriteStatement(left), rewriteStatement(right))
      case CTE(defLabel, defAlias, defQuery, matHint, useQuery) =>
        CTE(defLabel, defAlias, rewriteStatement(defQuery), matHint, rewriteStatement(useQuery))
      case v: Values =>
        v
      case s@Select(_distinctiveness, _selectList, rawFrom, where, _groupBy, _having, _orderBy, _limit, _offset, Some(search), _hint) =>
        val from = rewriteFrom(rawFrom)
        if(searchBeforeQuery) {
          val additionalTerm = rewriteSearch(from, search)
          s.copy(from = from, where = Some(where.fold(additionalTerm)(mkAnd(additionalTerm, _))), search = None)
        } else {
          // This is Complicated because I feel like such a search
          // should apply after everything _except_ the
          // distinct-on-ness.
          //
          // So, we'll want to rewrite things, in the absence of "distinct on" into
          //    select ..., row_number() over () from {everything except limit, offset, search}
          //    |> select ... order by row_number [limit l] [offset o]
          // or, with distinct on,
          //    select indistinct ..., row_number() over () from {everything except limit, offset, search}
          //    |> select distinct on(relevant) ... order by (relevant), row_number [limit l] [offset o]
          ???
        }
      case s: Select =>
        s.copy(from = rewriteFrom(s.from))
    }

  private def rewriteFrom(from: From): From =
    from.map[MT](
      rewriteAtomicFrom,
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, rewriteAtomicFrom(right), on) }
    )

  private def rewriteAtomicFrom(from: AtomicFrom): AtomicFrom =
    from match {
      case ft: FromTable => ft
      case fsr: FromSingleRow => fsr
      case FromStatement(statement, label, resourceName, alias) =>
        FromStatement(rewriteStatement(statement), label, resourceName, alias)
    }

  private def rewriteSearch(from: From, search: String): Expr =
    from.reduce[Expr](
      rewriteSearchAF(_, search),
      { (expr, join) => mkOr(expr, rewriteSearchAF(join.right, search)) }
    )

  private def rewriteSearchAF(from: AtomicFrom, search: String): Expr = {
    val columns: Iterator[Expr] =
      from match {
        case fsr: FromSingleRow =>
          Iterator.empty
        case ft: FromTable =>
          ft.columns.iterator.flatMap { case (dcn, FromTable.ColumnInfo(_, typ, _hint)) =>
            fieldsOf(PhysicalColumn[MT](ft.label, ft.tableName, dcn, typ)(AtomicPositionInfo.Synthetic))
          }
        case fs: FromStatement =>
          fs.statement.schema.iterator.flatMap { case (acl, Statement.SchemaEntry(_, typ, _hint, isSynthetic)) =>
            if(isSynthetic) {
              Nil
            } else {
              fieldsOf(VirtualColumn[MT](fs.label, acl, typ)(AtomicPositionInfo.Synthetic))
            }
          }
      }

    if(columns.hasNext) {
      val haystack =
        columns.map(denull).reduceLeft { (left, right) => // || is left-associative
          assert(isText(left.typ))
          assert(isText(right.typ))

          val result =
            FunctionCall[MT](
              concat,
              Seq(
                FunctionCall[MT](
                  concat,
                  Seq(left, litText(" "))
                )(FuncallPositionInfo.Synthetic),
                right
              )
            )(FuncallPositionInfo.Synthetic)

          assert(isText(result.typ))

          result
        }
      val needle = litText(search)

      FunctionCall[MT](
        tsSearch,
        Seq(
          FunctionCall[MT](toTsVector, Seq(haystack))(FuncallPositionInfo.Synthetic),
          FunctionCall[MT](plainToTsQuery, Seq(needle))(FuncallPositionInfo.Synthetic)
        )
      )(FuncallPositionInfo.Synthetic)
    } else {
      litBool(false)
    }
  }

  protected def litText(s: String): Expr
  protected def litBool(b: Boolean): Expr

  protected def isText(t: CT): Boolean
  protected def isBoolean(t: CT): Boolean

  protected def fieldsOf(expr: Expr): Seq[Expr]
  protected def mkAnd(left: Expr, right: Expr): Expr
  protected def mkOr(left: Expr, right: Expr): Expr
  protected def denull(string: Expr): Expr
  protected def concat: MonomorphicFunction
  protected def tsSearch: MonomorphicFunction
  protected def toTsVector: MonomorphicFunction
  protected def plainToTsQuery: MonomorphicFunction
}
