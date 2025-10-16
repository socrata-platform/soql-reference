package com.socrata.soql.sqlizer

import scala.collection.compat.immutable.LazyList

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._
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
      case CTE(defns, useQuery) =>
        val newDefns = defns.withValuesMapped { defn => defn.copy(query = rewriteStatement(defn.query)) }
        CTE(newDefns, rewriteStatement(useQuery))
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
      case fc: FromCTE => fc
      case FromStatement(statement, label, resourceName, canonicalName, alias) =>
        FromStatement(rewriteStatement(statement), label, resourceName, canonicalName, alias)
    }

  private def rewriteSearch(from: From, search: String): Expr =
    from.reduce[Expr](
      rewriteSearchAF(_, search),
      { (expr, join) => mkOr(expr, rewriteSearchAF(join.right, search)) }
    )

  private def rewriteSearchAF(from: AtomicFrom, search: String): Expr = {
    def findColumns(fieldExtract: Expr => Seq[Expr]) =
      from match {
        case fsr: FromSingleRow =>
          Iterator.empty
        case ft: FromTable =>
          // ok, this is annoying and _potentially_ can cause the same
          // query to give different results in different secondaries,
          // but it's necessary to make such a query use the FTS index
          // that we build.  We're going to order the columns by their
          // database column name...
          ft.columns
            .toVector
            .sortWith { (c1, c2) => compareDatabseColumnNames(c1._1, c2._1) }
            .iterator
            .flatMap { case (dcn, FromTable.ColumnInfo(_, typ, _hint)) =>
            fieldExtract(PhysicalColumn[MT](ft.label, ft.tableName, dcn, typ)(AtomicPositionInfo.Synthetic))
          }
        case fs: FromStatement =>
          fs.statement.schema.iterator.flatMap { case (acl, Statement.SchemaEntry(_, typ, _hint, isSynthetic)) =>
            if(isSynthetic) {
              Nil
            } else {
              fieldExtract(VirtualColumn[MT](fs.label, acl, typ)(AtomicPositionInfo.Synthetic))
            }
          }
        case fc: FromCTE =>
          fc.basedOn.schema.iterator.flatMap { case (acl, Statement.SchemaEntry(_, typ, _hint, isSynthetic)) =>
            if(isSynthetic) {
              Nil
            } else {
              fieldExtract(VirtualColumn[MT](fc.label, fc.columnMapping(acl), typ)(AtomicPositionInfo.Synthetic))
            }
          }
      }

    val textExpr: Option[Expr] = locally {
      val textColumns: Iterator[Expr] = findColumns(textFieldsOf)
      if(textColumns.hasNext) {
        val haystack =
          textColumns.map(denull).reduceLeft { (left, right) => // || is left-associative
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

        val finalized =
          FunctionCall[MT](
            tsSearch,
            Seq(
              FunctionCall[MT](toTsVector, Seq(haystack))(FuncallPositionInfo.Synthetic),
              FunctionCall[MT](plainToTsQuery, Seq(needle))(FuncallPositionInfo.Synthetic)
            )
          )(FuncallPositionInfo.Synthetic)

        Some(finalized)
      } else {
        None
      }
    }

    val numExpr: Iterator[Expr] = litNum(search).iterator.flatMap { numLiteral =>
      findColumns(numberFieldsOf).map { numCol =>
        mkEq(numCol, numLiteral)
      }
    }

    if(numExpr.hasNext) {
      val folder =
        textExpr match {
          case Some(te) =>
            numExpr.foldLeft(te) _
          case None =>
            numExpr.reduceLeft _
        }

      folder { (acc, numTest) =>
        mkOr(acc, numTest)
      }
    } else {
      textExpr.getOrElse(litBool(false))
    }
  }

  // returns true if "a" is less than "b"
  protected def compareDatabseColumnNames(a: DatabaseColumnName, b: DatabaseColumnName): Boolean

  type NumberLiteral
  protected def litText(s: String): Expr
  protected def litBool(b: Boolean): Expr
  protected def litNum(s: String): Option[NumberLiteral]

  protected def isText(t: CT): Boolean
  protected def isBoolean(t: CT): Boolean

  protected def textFieldsOf(expr: Expr): Seq[Expr]
  protected def numberFieldsOf(expr: Expr): Seq[Expr]
  protected def mkAnd(left: Expr, right: Expr): Expr
  protected def mkOr(left: Expr, right: Expr): Expr
  protected def mkEq(left: Expr, right: NumberLiteral): Expr
  protected def denull(string: Expr): Expr
  protected def concat: MonomorphicFunction
  protected def tsSearch: MonomorphicFunction
  protected def toTsVector: MonomorphicFunction
  protected def plainToTsQuery: MonomorphicFunction
}
