package com.socrata.soql.analyzer2

import scala.util.parsing.input.NoPosition

import com.socrata.soql.{BinaryTree, Leaf, UnionQuery, UnionAllQuery, IntersectQuery, IntersectAllQuery, MinusQuery, MinusAllQuery, PipeQuery}
import com.socrata.soql.ast
import com.socrata.soql.environment.{ColumnName, TableName}

class SoQLApproximation[MT <: MetaTypes] private (ops: SoQLApproximation.MetaOps[MT]) extends StatementUniverse[MT] {
  def convertStmt(stmt: Statement): BinaryTree[ast.Select] = {
    stmt match {
      case _: CTE =>
        throw new Exception("There is no source form for CTEs yet")

      case CombinedTables(op, left, right) =>
        val newLeft = convertStmt(left)
        val newRight = convertStmt(right)
        op match {
          case TableFunc.Union => UnionQuery(newLeft, newRight)
          case TableFunc.UnionAll => UnionAllQuery(newLeft, newRight)
          case TableFunc.Intersect => IntersectQuery(newLeft, newRight)
          case TableFunc.IntersectAll => IntersectAllQuery(newLeft, newRight)
          case TableFunc.Minus => MinusQuery(newLeft, newRight)
          case TableFunc.MinusAll => MinusAllQuery(newLeft, newRight)
        }

      case _: Values =>
        throw new Exception("No source form for VALUES yet")

      case Select(
        distinctiveness,
        selectList,
        from,
        where,
        groupBy,
        having,
        orderBy,
        limit,
        offset,
        search,
        hint
      ) =>
        val (from0, joinsReversed) =
          from.reduce[(AtomicFrom, List[ast.Join])](
            { leftmost => (leftmost, Nil) },
            { case ((leftmost, joinsReversed), Join(typ, lateral, left, right, on)) =>
              val newRight: ast.JoinSelect = right match {
                case ft: FromTable => ast.JoinTable(TableName(ops.databaseTableName(ft.tableName), Some(convertAutoTableLabel(ft.label))))
                case fsr: FromSingleRow => ast.JoinTable(TableName(TableName.SingleRow, Some(convertAutoTableLabel(fsr.label))))
                case fs: FromStatement => ast.JoinQuery(convertStmt(fs.statement), convertAutoTableLabel(fs.label))
                case fc: FromCTE => ast.JoinQuery(convertStmt(fc.basedOn), convertAutoTableLabel(fc.label))
              }
              (leftmost, ast.Join(convertJoinType(typ), newRight, convertExpr(on), lateral) :: joinsReversed)
            }
          )

        val baseSelect = ast.Select(
          distinctiveness match {
            case Distinctiveness.Indistinct() => ast.Indistinct
            case Distinctiveness.FullyDistinct() => ast.FullyDistinct
            case Distinctiveness.On(exprs) => ast.DistinctOn(exprs.map(convertExpr))
          },
          ast.Selection(
            None,
            Nil,
            selectList.map { case (label, namedExpr) =>
              ast.SelectedExpression(
                convertExpr(namedExpr.expr),
                Some((convertAutoColumnLabel(label), NoPosition))
              )
            }.toSeq
          ),
          from0 match {
            case ft: FromTable => Some(TableName(ops.databaseTableName(ft.tableName), Some(convertAutoTableLabel(ft.label))))
            case fsr: FromSingleRow => Some(TableName(TableName.SingleRow, Some(convertAutoTableLabel(fsr.label))))
            case fs: FromStatement => None
            case fc: FromCTE => None
          },
          joinsReversed.reverse,
          where.map(convertExpr),
          groupBy.map(convertExpr),
          having.map(convertExpr),
          orderBy.map(convertOrderBy),
          limit,
          offset,
          search,
          hint.map(convertSelectHint).toSeq
        )

        from0 match {
          case fs: FromStatement => PipeQuery(convertStmt(fs.statement), Leaf(baseSelect.copy(from = Some(TableName(TableName.This, Some(convertAutoTableLabel(fs.label)))))))
          case fc: FromCTE => PipeQuery(convertStmt(fc.basedOn), Leaf(baseSelect.copy(from = Some(TableName(TableName.This, Some(convertAutoTableLabel(fc.label)))))))
          case _ : FromTable => Leaf(baseSelect)
          case _ : FromSingleRow => Leaf(baseSelect)
        }
    }
  }

  private def convertJoinType(typ: JoinType): ast.JoinType =
    typ match {
      case JoinType.Inner => ast.InnerJoinType
      case JoinType.LeftOuter => ast.LeftOuterJoinType
      case JoinType.RightOuter => ast.RightOuterJoinType
      case JoinType.FullOuter => ast.FullOuterJoinType
    }

  private def convertExpr(e: Expr): ast.Expression = {
    e match {
      case lv@LiteralValue(_value) =>
        ops.literalValue(lv)
      case nl@NullLiteral(_typ) =>
        ast.NullLiteral()(nl.position.source.position)
      case pc@PhysicalColumn(table, _tableName, column, _typ) =>
        ast.ColumnOrAliasRef(Some(convertAutoTableLabel(table)), ops.databaseColumnName(column))(pc.position.source.position)
      case vc@VirtualColumn(table, column, _typ) =>
        ast.ColumnOrAliasRef(Some(convertAutoTableLabel(table)), convertAutoColumnLabel(column))(vc.position.source.position)
      case fc@FunctionCall(func, args) =>
        ast.FunctionCall(func.name, args.map(convertExpr))(fc.position.source.position, fc.position.functionNameSource.position)
      case afc@AggregateFunctionCall(func, args, distinct, filter) =>
        if(distinct) {
          throw new Exception("Cannot convert a FunctionCall with a DISTINCT to an AST")
        }
        ast.FunctionCall(func.name, args.map(convertExpr), filter.map(convertExpr))(afc.position.source.position, afc.position.functionNameSource.position)
      case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) =>
        // uggghhghgh
        val frameStuff = frame.map { frame =>
          val frameContext =
            frame.context match {
              case FrameContext.Range => ast.StringLiteral("RANGE")(NoPosition)
              case FrameContext.Rows => ast.StringLiteral("ROWS")(NoPosition)
              case FrameContext.Groups => ast.StringLiteral("GROUPS")(NoPosition)
            }

          val frameBound =
            frame.end match {
              case Some(end) =>
                Seq(ast.StringLiteral("BETWEEN")(NoPosition)) ++
                  convertFrameBound(frame.start) ++
                  Seq(ast.StringLiteral("AND")(NoPosition)) ++
                  convertFrameBound(end)
              case None =>
                convertFrameBound(frame.start)
            }

          val frameExclusion = frame.exclusion.map(convertFrameExclusion).getOrElse(Nil)

          Seq(frameContext) ++ frameBound ++ frameExclusion
        }.getOrElse(Nil)

        ast.FunctionCall(
          func.name,
          args.map(convertExpr),
          filter.map(convertExpr),
          Some(ast.WindowFunctionInfo(
                 partitionBy.map(convertExpr),
                 orderBy.map(convertOrderBy),
                 frameStuff
               ))
        )(wfc.position.source.position, wfc.position.functionNameSource.position)

      case _ : SelectListReference =>
        throw new Exception("Cannot get a soql approximation for a select list reference")
    }
  }

  private def convertFrameBound(bound: FrameBound) =
    bound match {
      case FrameBound.UnboundedPreceding => Seq("UNBOUNDED", "PRECEDING").map(ast.StringLiteral(_)(NoPosition))
      case FrameBound.Preceding(n) => Seq(ast.NumberLiteral(n)(NoPosition), ast.StringLiteral("PRECEDING")(NoPosition))
      case FrameBound.CurrentRow => Seq("CURRENT", "ROW").map(ast.StringLiteral(_)(NoPosition))
      case FrameBound.Following(n) => Seq(ast.NumberLiteral(n)(NoPosition), ast.StringLiteral("FOLLOWING")(NoPosition))
      case FrameBound.UnboundedFollowing => Seq("UNBOUNDED", "FOLLOWING").map(ast.StringLiteral(_)(NoPosition))
    }

  private def convertFrameExclusion(exclusion: FrameExclusion) =
    exclusion match {
      case FrameExclusion.CurrentRow => Seq("EXCLUDE","CURRENT","ROW").map(ast.StringLiteral(_)(NoPosition))
      case FrameExclusion.Group => Seq("EXCLUDE","GROUP").map(ast.StringLiteral(_)(NoPosition))
      case FrameExclusion.Ties => Seq("EXCLUDE","TIES").map(ast.StringLiteral(_)(NoPosition))
      case FrameExclusion.NoOthers => Seq("EXCLUDE","NO","OTHERS").map(ast.StringLiteral(_)(NoPosition))
    }

  private def convertOrderBy(ob: OrderBy): ast.OrderBy = {
    val OrderBy(expr, ascending, nullLast) = ob
    ast.OrderBy(convertExpr(expr), ascending, nullLast)
  }

  private def convertAutoTableLabel(label: AutoTableLabel): String =
    s"@${ops.auto_table_label_prefix}${label.name}"

  private def convertAutoColumnLabel(label: AutoColumnLabel): ColumnName =
    ColumnName(s"${ops.auto_column_label_prefix}${label.name}")

  private def convertSelectHint(hint: SelectHint): ast.Hint =
    hint match {
      case SelectHint.Materialized => ast.Materialized(NoPosition)
      case SelectHint.NoRollup => ast.NoRollup(NoPosition)
      case SelectHint.NoChainMerge => ast.NoChainMerge(NoPosition)
      case SelectHint.CompoundRollup => ast.CompoundRollup(NoPosition)
      case SelectHint.RollupAtJoin => ast.RollupAtJoin(NoPosition)
    }
}

object SoQLApproximation {
  trait MetaOps[MT <: MetaTypes] extends StatementUniverse[MT] {
    def databaseTableName(dtn: DatabaseTableName): String
    def databaseColumnName(dtn: DatabaseColumnName): ColumnName
    def literalValue(value: LiteralValue): ast.Expression

    def auto_table_label_prefix = "approx_autogen_table_name_"
    def auto_column_label_prefix = "approx_autogen_column_name_"
  }

  def apply[MT <: MetaTypes](analysis: SoQLAnalysis[MT], ops: MetaOps[MT]): BinaryTree[ast.Select] =
    new SoQLApproximation(ops).convertStmt(analysis.removeSelectListReferences.statement)
}
