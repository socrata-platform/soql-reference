// TODO:
//  * Errors
//     - everything needs position info

package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.control.NoStackTrace
import scala.util.parsing.input.{Position, NoPosition}
import scala.collection.compat._
import scala.collection.compat.immutable.LazyList

import java.util.concurrent.atomic.AtomicBoolean

import com.rojoma.json.v3.ast.JValue

import com.socrata.soql.{BinaryTree, Leaf, TrueOp, Compound, PipeQuery, UnionQuery, UnionAllQuery, IntersectQuery, IntersectAllQuery, MinusQuery, MinusAllQuery}
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.ast
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, ScopedResourceName, Source, TableName, HoleName, UntypedDatasetContext, FunctionName, Provenance}
import com.socrata.soql.typechecker.{TypeInfo2, FunctionInfo}
import com.socrata.soql.aliases.AliasAnalysis
import com.socrata.soql.exceptions.AliasAnalysisException

object SoQLAnalyzer {
  private val This = ResourceName("this")
  private val SingleRow = ResourceName("single_row")

  private val SpecialNames = Set(This, SingleRow)
}

class SoQLAnalyzer[MT <: MetaTypes] private (
  typeInfo: TypeInfo2[MT],
  functionInfo: FunctionInfo[MT#ColumnType],
  toProvenance: ToProvenance[MT#DatabaseTableNameImpl],
  aggregateMerge: Option[(ColumnName, Expr[MT]) => Option[Expr[MT]]],
  allowSloppyUDFParams: Boolean
) extends StatementUniverse[MT] {
  def this(
    typeInfo: TypeInfo2[MT],
    functionInfo: FunctionInfo[MT#ColumnType],
    toProvenance: ToProvenance[MT#DatabaseTableNameImpl]
  ) =
    this(typeInfo, functionInfo, toProvenance, None, allowSloppyUDFParams = false)

  type TableMap = com.socrata.soql.analyzer2.TableMap[MT]
  type FoundTables = com.socrata.soql.analyzer2.FoundTables[MT]
  type TableDescription = com.socrata.soql.analyzer2.TableDescription[MT]
  type UserParameters = com.socrata.soql.analyzer2.UserParameters[CT, CV]
  type UserParameterSpecs = com.socrata.soql.analyzer2.UserParameterSpecs[CT]

  private case class Bail(result: SoQLAnalyzerError[RNS]) extends Exception with NoStackTrace

  private val Error = SoQLAnalyzerError

  private def preserveSystemColumnsRequested =
    aggregateMerge.isDefined

  private def copy(
    aggregateMerge: Option[(ColumnName, Expr) => Option[Expr]] = aggregateMerge,
    allowSloppyUDFParams: Boolean = allowSloppyUDFParams
  ): SoQLAnalyzer[MT] =
    new SoQLAnalyzer(typeInfo, functionInfo, toProvenance, aggregateMerge, allowSloppyUDFParams)

  def preserveSystemColumns(aggregateMerge: (ColumnName, Expr) => Option[Expr]): SoQLAnalyzer[MT] =
    copy(aggregateMerge = Some(aggregateMerge))

  def sloppyUDFParams(allow: Boolean) =
    copy(allowSloppyUDFParams = allow)

  def apply(start: FoundTables, userParameters: UserParameters): Either[SoQLAnalyzerError[RNS], SoQLAnalysis[MT]] = {
    try {
      validateUserParameters(start.knownUserParameters, userParameters)

      val state = new State(start.tableMap, userParameters)

      Right(new SoQLAnalysis[MT](
        state.labelProvider,
        state.analyze(start.initialScope, start.initialQuery)
      ))
    } catch {
      case Bail(result) =>
        Left(result)
    }
  }

  private def validateUserParameters(specs: UserParameterSpecs, params: UserParameters): Unit = {
    // what we're checking here is that all params given which match a
    // param that exists in the specs, matches the type.  We don't
    // care about extra parameters (they'll just be ignored) and we
    // don't care about missing parameters (they'll be complained
    // about, if relevant, when encountered while typechecking) but we
    // _do_ care that a user parameter can't change the meaning of a
    // query by altering what function overload gets selected during
    // typechecking.

    val UserParameters(qualified, unqualified) = params
    for {
      (canonName, params) <- qualified
      spec <- specs.qualified.get(canonName)
      (param, value) <- params
      expectedType <- spec.get(param)
      givenType = value match {
        case UserParameters.Null(ct) => ct
        case UserParameters.Value(v) => typeInfo.hasType.typeOf(v)
      }
      if givenType != expectedType
    } {
      throw Bail(SoQLAnalyzerError.InvalidParameterType(Some(canonName), param, typeInfo.typeNameFor(expectedType), typeInfo.typeNameFor(givenType)))
    }

    (specs.unqualified, params.unqualified) match {
      case (Left(expectedCanonName), _) =>
      case (Right(spec), params) =>
        for {
          (param, value) <- params
          expectedType <- spec.get(param)
          givenType = value match {
            case UserParameters.Null(ct) => ct
            case UserParameters.Value(v) => typeInfo.hasType.typeOf(v)
          }
          if givenType != expectedType
        } {
          throw Bail(SoQLAnalyzerError.InvalidParameterType(None, param, typeInfo.typeNameFor(expectedType), typeInfo.typeNameFor(givenType)))
        }
    }
  }

  private final class State(tableMap: TableMap, userParameters: UserParameters) {
    val labelProvider = new LabelProvider

    sealed abstract class ImplicitFrom {
      def optionalize(left: Boolean): ImplicitFrom
      def forced: Boolean
    }
    object ImplicitFrom {
      // "none" means "there is no implicit FROM; the current soql
      // query _must_ provide a FROM clause and it cannot be
      // "FROM @this"
      case object None extends ImplicitFrom {
        def optionalize(left: Boolean) = this
        def forced = true
      }
      // "required" means "there is an implicit FROM; the current soql
      // query _must not_ provide one that queries something else, but
      // _may_ do "FROM @this AS @alias"
      case class Required(from: AtomicFrom) extends ImplicitFrom {
        def optionalize(left: Boolean) = new Optional(from, left)
        def forced = true
      }
      // "optional" means "there is an implicit FROM, but it is not
      // required to be used in the current context; the current soql
      // query may do _either_ a FROMless select, a "FROM @this
      // AS @alias" or "FROM (something_else)".  This is used in
      // combined-tables queries (union etc) to specify that you _may_
      // refer to the context query but you don't _have_ to refer to
      // it in any particular subquery.
      //
      // I would like to make it so that you _have_ to say
      // "FROM @this" if you want to use the context query in such a
      // subquery, but I know for a fact that would break existing
      // queries which use the context implicitly in the leftmost
      // subquery, and I'm unconvinced it wouldn't break existing
      // queries in other positions too; this value controls whether
      // fromless selects are allowed in non-leftmost positions (note
      // the "can use nothing on the RHS of a table op" test over in
      // SoQLAnalyzerTest checks the behavior controlled by this
      // switch):
      val allowNonLeftmostImplicitFromInTableOp = true
      //
      // This state dance is because we only need to guarantee labels'
      // uniqueness if the optional implicit from is actually used,
      // and we only care that the context query is used in _at least
      // one_ subtree.
      class Optional(underlying: AtomicFrom, val leftmost: Boolean, forcedOnce: AtomicBoolean = new AtomicBoolean(false)) extends ImplicitFrom {
        lazy val get: AtomicFrom = locally {
          forcedOnce.set(true)
          underlying.relabel(labelProvider)
        }
        def optionalize(newLeft: Boolean) = new Optional(underlying, leftmost && newLeft, forcedOnce)
        def forced = forcedOnce.get
      }
    }

    def analyze(scope: RNS, query: FoundTables.Query[MT]): Statement = {
      val ctx = Ctx(scope, None, None, primaryTableName(scope, query), Environment.empty, Map.empty, Set.empty, Map.empty, preserveSystemColumnsRequested)
      val from =
        query match {
          case FoundTables.Saved(rn) =>
            analyzeForFrom(ctx.scopedResourceName, ScopedResourceName(scope, rn), None, NoPosition)
          case FoundTables.InContext(rn, q, _, parameters) =>
            val from = analyzeForFrom(ctx.scopedResourceName, ScopedResourceName(scope, rn), None, NoPosition)
            analyzeStatement(ctx, q, ImplicitFrom.Required(from))
          case FoundTables.InContextImpersonatingSaved(rn, q, _, parameters, impersonating) =>
            val from = analyzeForFrom(ctx.scopedResourceName, ScopedResourceName(scope, rn), None, NoPosition)
            analyzeStatement(ctx.copy(canonicalName = Some(impersonating)), q, ImplicitFrom.Required(from))
          case FoundTables.Standalone(q, _, parameters) =>
            analyzeStatement(ctx, q, ImplicitFrom.None)
        }
      intoStatement(from)
    }

    def primaryTableName(scope: RNS, query: FoundTables.Query[MT]): Option[Provenance] =
      query match {
        case FoundTables.Saved(rn) =>
          primaryTableName(ScopedResourceName(scope, rn))
        case FoundTables.InContext(rn, q, _, parameters) =>
          primaryTableName(ScopedResourceName(scope, rn))
        case FoundTables.InContextImpersonatingSaved(rn, q, _, parameters, impersonating) =>
          primaryTableName(ScopedResourceName(scope, rn))
        case FoundTables.Standalone(q, _, parameters) =>
          primaryTableName(scope, q)
      }

    def primaryTableName(name: ScopedResourceName): Option[Provenance] =
      tableMap.get(name).flatMap { desc =>
        desc match {
          case ds: TableDescription.Dataset[MT] => Some(toProvenance.toProvenance(ds.name))
          case q: TableDescription.Query[MT] => primaryTableName(ScopedResourceName(q.scope, q.basedOn))
          case f: TableDescription.TableFunction[MT] => primaryTableName(f.scope, f.parsed)
        }
      }

    @tailrec
    def primaryTableName(scope: RNS, tree: BinaryTree[ast.Select]): Option[Provenance] =
      tree match {
        case Leaf(select) => primaryTableName(scope, select)
        case compound : Compound[ast.Select] => primaryTableName(scope, compound.left)
      }

    def primaryTableName(scope: RNS, select: ast.Select): Option[Provenance] =
      select.from.flatMap { tn: TableName =>
        val rn = ResourceName(tn.nameWithoutPrefix)
        primaryTableName(ScopedResourceName(scope, rn))
      }

    def intoStatement(from: AtomicFrom): Statement = {
      from match {
        case from: FromTable =>
          selectFromFrom(
            from.columns.map { case (label, FromTable.ColumnInfo(name, typ, _hint)) =>
              labelProvider.columnLabel() -> NamedExpr(PhysicalColumn[MT](from.label, from.tableName, label, typ)(AtomicPositionInfo.Synthetic), name, None, isSynthetic = false)
            },
            from
          )
        case from: FromSingleRow =>
          selectFromFrom(OrderedMap.empty[AutoColumnLabel, NamedExpr], from)
        case from: FromStatement =>
          // Just short-circuit it and return the underlying Statement
          from.statement
      }
    }

    def selectFromFrom(
      selectList: OrderedMap[AutoColumnLabel, NamedExpr],
      from: AtomicFrom
    ): Statement = {
      Select(
        distinctiveness = Distinctiveness.Indistinct(),
        selectList = selectList,
        from = from,
        where = None,
        groupBy = Nil,
        having = None,
        orderBy = Nil,
        limit = None,
        offset = None,
        search = None,
        hint = Set.empty
      )
    }

    def fromTable(srn: ScopedResourceName, desc: TableDescription.Dataset[MT]): AtomicFrom = {
      if(desc.ordering.isEmpty) {
        FromTable(
          desc.name,
          srn,
          None,
          labelProvider.tableLabel(),
          columns = desc.schema.filter { case (_, FromTable.ColumnInfo(name, _, _)) => !desc.hiddenColumns(name) },
          primaryKeys = desc.primaryKeys
        )
      } else {
        for(TableDescription.Ordering(col, _ascending, _nullLast) <- desc.ordering) {
          val typ = desc.schema(col).typ
          if(!typeInfo.isOrdered(typ)) {
            // should this be Synthetic instead of saved...?
            throw Bail(SoQLAnalyzerError.TypecheckError.UnorderedOrderBy(Source.Saved(srn, NoPosition), typeInfo.typeNameFor(typ)))
          }
        }

        val from = FromTable(
          desc.name,
          srn,
          None,
          labelProvider.tableLabel(),
          columns = desc.schema,
          primaryKeys = desc.primaryKeys
        )

        val columnLabels = from.columns.map { case _ => labelProvider.columnLabel() }

        FromStatement(
          Select(
            Distinctiveness.Indistinct(),
            selectList = OrderedMap() ++ from.columns.iterator.zip(columnLabels.iterator).flatMap { case ((dcn, FromTable.ColumnInfo(name, typ, _hint)), outputLabel) =>
              if(desc.hiddenColumns(name)) {
                None
              } else {
                Some(outputLabel -> NamedExpr(PhysicalColumn[MT](from.label, from.tableName, dcn, typ)(AtomicPositionInfo.Synthetic), name, hint = None, isSynthetic = false))
              }
            },
            from,
            None,
            Nil,
            None,
            desc.ordering.map { case TableDescription.Ordering(dcn, ascending, nullLast) =>
              val FromTable.ColumnInfo(_, typ, _) = from.columns(dcn)
              OrderBy(PhysicalColumn[MT](from.label, from.tableName, dcn, typ)(AtomicPositionInfo.Synthetic), ascending = ascending, nullLast = nullLast)
            },
            None,
            None,
            None,
            Set.empty
          ),
          labelProvider.tableLabel(),
          Some(srn),
          None
        )
      }
    }

    def analyzeForFrom(source: Option[ScopedResourceName], name: ScopedResourceName, canonicalName: Option[CanonicalName], position: Position): AtomicFrom = {
      if(name.name == SoQLAnalyzer.SingleRow) {
        FromSingleRow(labelProvider.tableLabel(), None)
      } else if(name.name == SoQLAnalyzer.This) {
        // analyzeSelection will handle @this in the correct
        // position...
        illegalThisReference(source, position)
      } else {
        tableMap.find(name) match {
          case ds: TableDescription.Dataset[MT] =>
            fromTable(name, ds)
          case TableDescription.Query(scope, canonicalName, basedOn, parsed, _unparsed, parameters, hiddenColumns, outputColumnHints) =>
            // so this is basedOn |> parsed
            // so we want to use "basedOn" as the implicit "from" for "parsed"
            val from = analyzeForFrom(source, ScopedResourceName(scope, basedOn), None, NoPosition /* Yes, actually NoPosition here */)
            analyzeStatement(Ctx(scope, Some(name), Some(canonicalName), primaryTableName(ScopedResourceName(scope, basedOn)), Environment.empty, Map.empty, hiddenColumns, outputColumnHints, true), parsed, ImplicitFrom.Required(from))
          case TableDescription.TableFunction(_, _, _, _, _, _) =>
            parameterlessTableFunction(source, name.name, position)
        }
      }
    }

    // This is the current ambient environment in which SoQL analysis
    // occurs.
    case class Ctx(
      // The scope in which names found in the current soql under
      // inspection will be looked up.
      scope: RNS,
      // The current scoped resource name; note the scope here is not
      // necessarily the same as the scope above if we're crossing a
      // scope boundary!  This is just used to decorate the typed trees
      // with source information.
      scopedResourceName: Option[ScopedResourceName],
      // The canonical name of the source of the soql currently being
      // analyzed.  This is used to look up unqualified user
      // parameters' values.
      canonicalName: Option[CanonicalName],
      // The provenance of the primary underlying table being analyzed
      primaryTableName: Option[Provenance],
      // The current environment in which possibly-qualified
      // column-names can be matched to input columns.
      enclosingEnv: Environment[MT],
      // Any UDF parameters available for the current soql.
      udfParams: Map[HoleName, (Option[ScopedResourceName], Position) => Expr],
      // What output columns generated by the current SoQL should be
      // marked as hidden.
      hiddenColumns: Set[ColumnName],
      // Hints to attach to output columns
      outputColumnHints: Map[ColumnName, JValue],
      // Whether we should attempt to preserve system columns from our
      // primary input table.
      attemptToPreserveSystemColumns: Boolean
    ) {
      private def error(e: SoQLAnalyzerError[RNS]): Nothing =
        throw Bail(e)

      def expectedBoolean(expr: ast.Expression, got: CT): Nothing =
        error(Error.ExpectedBoolean(Source.nonSynthetic(scopedResourceName, expr.position), typeInfo.typeNameFor(got)))
      def incorrectNumberOfParameters(forUdf: ResourceName, expected: Int, got: Int, position: Position): Nothing =
        error(Error.IncorrectNumberOfUdfParameters(Source.nonSynthetic(scopedResourceName, position), forUdf, expected, got))
      def distinctOnMustBePrefixOfOrderBy(source: Source): Nothing =
        error(Error.DistinctOnNotPrefixOfOrderBy(source))
      def orderByMustBeSelected(source: Source): Nothing =
        error(Error.OrderByMustBeSelectedWhenDistinct(source))
      def invalidGroupBy(typ: CT, source: Source): Nothing =
        error(Error.InvalidGroupBy(source, typeInfo.typeNameFor(typ)))
      def unorderedOrderBy(typ: CT, source: Source): Nothing =
        error(Error.TypecheckError.UnorderedOrderBy(source, typeInfo.typeNameFor(typ)))
      def parametersForNonUdf(name: ResourceName, position: Position): Nothing =
        error(Error.ParametersForNonUDF(Source.nonSynthetic(scopedResourceName, position), name))
      def addScopeError(e: AddScopeError, position: Position): Nothing =
        e match {
          case AddScopeError.NameExists(n) =>
            error(Error.TableAliasAlreadyExists(Source.nonSynthetic(scopedResourceName, position), n))
          case AddScopeError.MultipleImplicit =>
            // This shouldn't be able to occur from user input - the
            // grammar disallows it.
            throw new Exception("Multiple implicit tables??")
        }
      def noDataSource(position: Position): Nothing =
        error(Error.FromRequired(Source.nonSynthetic(scopedResourceName, position)))
      def chainWithFrom(position: Position): Nothing =
        error(Error.FromForbidden(Source.nonSynthetic(scopedResourceName, position)))
      def fromThisWithoutContext(position: Position): Nothing =
        error(Error.FromThisWithoutContext(Source.nonSynthetic(scopedResourceName, position)))
      def tableOpTypeMismatch(left: OrderedMap[ColumnName, CT], right: OrderedMap[ColumnName, CT], position: Position): Nothing =
        error(Error.TableOperationTypeMismatch(Source.nonSynthetic(scopedResourceName, position), left.valuesIterator.map(typeInfo.typeNameFor).toVector, right.valuesIterator.map(typeInfo.typeNameFor).toVector))
      def literalNotAllowedInGroupBy(pos: Position): Nothing =
        error(Error.LiteralNotAllowedInGroupBy(Source.nonSynthetic(scopedResourceName, pos)))
      def literalNotAllowedInOrderBy(pos: Position): Nothing =
        error(Error.LiteralNotAllowedInOrderBy(Source.nonSynthetic(scopedResourceName, pos)))
      def literalNotAllowedInDistinctOn(pos: Position): Nothing =
        error(Error.LiteralNotAllowedInDistinctOn(Source.nonSynthetic(scopedResourceName, pos)))
      def aggregateFunctionNotAllowed(name: FunctionName, source: Source): Nothing =
        error(Error.AggregateFunctionNotAllowed(source, name))
      def ungroupedColumnReference(source: Source): Nothing =
        error(Error.UngroupedColumnReference(source))
      def windowFunctionNotAllowed(name: FunctionName, source: Source): Nothing =
        error(Error.WindowFunctionNotAllowed(source, name))
      def reservedTableName(name: ResourceName, pos: Position): Nothing =
        error(Error.ReservedTableName(Source.nonSynthetic(scopedResourceName, pos), name))
      def augmentAliasAnalysisException(aae: AliasAnalysisException): Nothing = {
        import com.socrata.soql.{exceptions => SE}

        aae match {
          case SE.RepeatedException(name, pos) =>
            error(Error.AliasAnalysisError.RepeatedExclusion(Source.nonSynthetic(scopedResourceName, pos), name))
          case SE.CircularAliasDefinition(name, pos) =>
            error(Error.AliasAnalysisError.CircularAliasDefinition(Source.nonSynthetic(scopedResourceName, pos), name))
          case SE.DuplicateAlias(name, pos) =>
            error(Error.AliasAnalysisError.DuplicateAlias(Source.nonSynthetic(scopedResourceName, pos), name))
          case nsc@SE.NoSuchColumn(name, pos) =>
            val qual = nsc.asInstanceOf[SE.NoSuchColumn.RealNoSuchColumn].qualifier.map(_.substring(1)).map(ResourceName(_)) // ew
            error(Error.TypecheckError.NoSuchColumn(Source.nonSynthetic(scopedResourceName, pos), qual, name, Util.possibilitiesFor(enclosingEnv, Iterator.empty, qual, name)))
          case SE.NoSuchTable(_, _) =>
            throw new Exception("Alias analysis doesn't actually throw NoSuchTable")
        }
      }
    }

    def analyzeStatement(ctx: Ctx, q: BinaryTree[ast.Select], from0: ImplicitFrom): FromStatement = {
      q match {
        case Leaf(select) =>
          analyzeSelection(ctx, select, from0)
        case PipeQuery(left, right) =>
          val analyzedLeft =
            analyzeStatement(
              Ctx(
                scope = ctx.scope,
                scopedResourceName = ctx.scopedResourceName,
                canonicalName = ctx.canonicalName,
                primaryTableName = ctx.primaryTableName,
                enclosingEnv = ctx.enclosingEnv,
                udfParams = ctx.udfParams,
                hiddenColumns = Set.empty,
                outputColumnHints = Map.empty,
                attemptToPreserveSystemColumns = preserveSystemColumnsRequested
              ),
              left, from0
            )
          analyzeStatement(ctx, right, ImplicitFrom.Required(analyzedLeft))
        case other: TrueOp[ast.Select] =>
          analyzeTableOp(ctx, other, from0)
      }
    }

    def analyzeTableOp(ctx: Ctx, op: TrueOp[ast.Select], from0: ImplicitFrom): FromStatement = {
      val subCtx =
        Ctx(
          scope = ctx.scope,
          scopedResourceName = None,
          canonicalName = None,
          primaryTableName = ctx.primaryTableName,
          enclosingEnv = ctx.enclosingEnv,
          udfParams = ctx.udfParams,
          hiddenColumns = Set.empty,
          outputColumnHints = ctx.outputColumnHints,
          attemptToPreserveSystemColumns = false
        )

      val lhsFrom = from0.optionalize(left = true)
      val lhs = intoStatement(analyzeStatement(subCtx, op.left, lhsFrom))
      val rhsFrom = from0.optionalize(left = false)
      val rhs = intoStatement(analyzeStatement(subCtx.copy(primaryTableName = primaryTableName(ctx.scope, op.right)), op.right, rhsFrom))

      if(from0.isInstanceOf[ImplicitFrom.Required] && !lhsFrom.forced && !rhsFrom.forced) {
        /* TODO: NEED POS INFO FROM AST - and what is the right position for saying "no one of these table-func'd subselects used the input query? */
        ctx.chainWithFrom(NoPosition)
      }

      if(lhs.schema.values.map(_.typ) != rhs.schema.values.map(_.typ)) {
        ctx.tableOpTypeMismatch(
          OrderedMap() ++ lhs.schema.valuesIterator.map { case Statement.SchemaEntry(name, typ, _hint, _isSynthetic) => name -> typ },
          OrderedMap() ++ rhs.schema.valuesIterator.map { case Statement.SchemaEntry(name, typ, _hint, _isSynthetic) => name -> typ },
          NoPosition /* TODO: NEED POS INFO FROM AST */
        )
      }

      val combined = CombinedTables(opify(op), lhs, rhs)
      val fromCombined = FromStatement(combined, labelProvider.tableLabel(), ctx.scopedResourceName, None)
      if(ctx.hiddenColumns.isEmpty) {
        fromCombined
      } else {
        // need to remove hidden columns
        val filteredStmt =
          Select(
            Distinctiveness.Indistinct(),
            OrderedMap() ++ combined.schema.flatMap { case (label, Statement.SchemaEntry(name, typ, hint, isSynthetic)) =>
              if(ctx.hiddenColumns(name)) {
                None
              } else {
                Some(labelProvider.columnLabel() -> NamedExpr(VirtualColumn[MT](fromCombined.label, label, typ)(AtomicPositionInfo.Synthetic), name, hint, isSynthetic))
              }
            },
            fromCombined,
            None, Nil, None, Nil, None, None, None, Set.empty
          )

        FromStatement(filteredStmt, labelProvider.tableLabel(), None, None)
      }
    }

    def opify(op: TrueOp[_]): TableFunc = {
      op match {
        case UnionQuery(_, _) => TableFunc.Union
        case UnionAllQuery(_, _) => TableFunc.UnionAll
        case IntersectQuery(_, _) => TableFunc.Intersect
        case IntersectAllQuery(_, _) => TableFunc.IntersectAll
        case MinusQuery(_, _) => TableFunc.Minus
        case MinusAllQuery(_, _) => TableFunc.MinusAll
      }
    }

    def isSystemColumn(name: ColumnName) = name.name.startsWith(":")

    @tailrec
    final def findSystemColumns(from: From): Iterable[(ColumnName, Column)] =
      from match {
        case j: Join => findSystemColumns(j.left)
        case FromTable(tableName, _, _, tableLabel, columns, _) =>
          columns.collect { case (colLabel, FromTable.ColumnInfo(name, typ, _hint)) if isSystemColumn(name) =>
            name -> PhysicalColumn[MT](tableLabel, tableName, colLabel, typ)(AtomicPositionInfo.Synthetic)
          }
        case FromStatement(stmt, tableLabel, _, _) =>
          stmt.schema.iterator.collect { case (colLabel, Statement.SchemaEntry(name, typ, _hint, _isSynthetic)) if isSystemColumn(name) =>
            name -> VirtualColumn[MT](tableLabel, colLabel, typ)(AtomicPositionInfo.Synthetic)
          }.toSeq
        case FromSingleRow(_, _) =>
          Nil
      }


    def addSystemColumnsIfDesired(select: Select, attemptToPreserveSystemColumns: Boolean): Select =
      aggregateMerge match {
        case Some(aggregateMerger) if attemptToPreserveSystemColumns =>
          select.distinctiveness match {
            case Distinctiveness.Indistinct() | Distinctiveness.On(_) =>
              val existingColumnNames = select.selectList.valuesIterator.map(_.name).to(Set)
              val newSelectList = select.selectList ++ findSystemColumns(select.from).flatMap { case (name, column) =>
                if(existingColumnNames(name)) {
                  None
                } else if(select.isAggregated) {
                  aggregateMerger(name, column).map { newExpr =>
                    labelProvider.columnLabel() -> NamedExpr(newExpr, name, hint = None, isSynthetic = true)
                  }
                } else {
                  Some(labelProvider.columnLabel() -> NamedExpr(column, name, hint = None, isSynthetic = true))
                }
              }
              select.copy(selectList = newSelectList)
            case Distinctiveness.FullyDistinct() =>
              // adding unrequested system columns would change
              // semantics on a fully distinct query, so don't
              select
          }
        case _ =>
          // haven't been asked to preserve system columns
          select
      }

    def analyzeSelection(ctx: Ctx, select: ast.Select, from0: ImplicitFrom): FromStatement = {
      val ast.Select(
        distinct,
        selection,
        from,
        joins,
        where,
        groupBys,
        having,
        orderBys,
        limit,
        offset,
        search,
        hints
      ) = select

      // ok, first things first: establish the schema we're
      // operating in, which means rolling up "from" and "joins"
      // into a single From
      val completeFrom = queryInputSchema(ctx, from0, from, joins)
      val localEnv = envify(ctx, completeFrom.extendEnvironment(ctx.enclosingEnv), NoPosition /* TODO: NEED POS INFO FROM AST */)

      // Now that we know what we're selecting from, we'll give names to the selection...
      val aliasAnalysis =
        try {
          AliasAnalysis(selection, from)(collectNamesForAnalysis(completeFrom))
        } catch {
          case e: AliasAnalysisException =>
            ctx.augmentAliasAnalysisException(e)
        }

      // With the aliases assigned we'll typecheck the select-list,
      // building up the set of named exprs we can use as shortcuts
      // for other expressions
      class EvaluationState(val allNamedExprs: Map[ColumnName, Expr] = OrderedMap.empty, val referencableNamedExprs: Map[ColumnName, Expr] = OrderedMap.empty) {
        def update(name: ColumnName, expr: Expr, enableReference: Boolean): EvaluationState = {
          new EvaluationState(
            allNamedExprs = allNamedExprs + (name -> expr),
            referencableNamedExprs =
              if(enableReference) referencableNamedExprs + (name -> expr)
              else referencableNamedExprs
          )
        }

        def typecheck(expr: ast.Expression, expectedType: Option[CT] = None) =
          State.this.typecheck(ctx.copy(enclosingEnv = localEnv), expr, referencableNamedExprs, expectedType)
      }

      val finalState = aliasAnalysis.evaluationOrder.foldLeft(new EvaluationState()) { (state, colName) =>
        val expression = aliasAnalysis.expressions(colName)
        val typed = state.typecheck(expression.expr)

        // A bit of old-analyzer compatibility here: In OA, you cannot
        // refer to a qualified column reference by its output name
        // unless that output name was explicitly given to it.
        //
        // There are views that depend on this behavior.
        def isQualifiedColumnReference = expression.expr match {
          case ast.ColumnOrAliasRef(Some(_), _) => true
          case _ => false
        }

        def hasExplicitAlias = expression.aliasType == AliasAnalysis.AliasType.Explicit

        state.update(colName, typed, enableReference = !isQualifiedColumnReference || hasExplicitAlias)
      }

      val checkedDistinct: Distinctiveness = distinct match {
        case ast.Indistinct => Distinctiveness.Indistinct()
        case ast.FullyDistinct => Distinctiveness.FullyDistinct()
        case ast.DistinctOn(exprs) =>
          Distinctiveness.On(
            exprs.map { expr =>
              if(expr.isInstanceOf[ast.Literal]) {
                ctx.literalNotAllowedInDistinctOn(expr.position)
              }
              finalState.typecheck(expr)
            }
          )
      }

      val checkedWhere = where.map(finalState.typecheck(_, Some(typeInfo.boolType)))
      val checkedGroupBys = groupBys.map { expr =>
        if(expr.isInstanceOf[ast.Literal]) {
          ctx.literalNotAllowedInGroupBy(expr.position)
        }
        val checked = finalState.typecheck(expr)
        if(!typeInfo.isGroupable(checked.typ)) {
          ctx.invalidGroupBy(checked.typ, checked.position.reference)
        }
        checked
      }
      val checkedHaving = having.map(finalState.typecheck(_, Some(typeInfo.boolType)))
      val checkedOrderBys = orderBys.map { case ast.OrderBy(expr, ascending, nullLast) =>
        if(expr.isInstanceOf[ast.Literal]) {
          ctx.literalNotAllowedInOrderBy(expr.position)
        }
        val checked = finalState.typecheck(expr)
        if(!typeInfo.isOrdered(checked.typ)) {
          ctx.unorderedOrderBy(checked.typ, checked.position.reference)
        }
        OrderBy(checked, ascending, nullLast)
      }

      checkedDistinct match {
        case Distinctiveness.On(exprs) =>
          for(expr <- exprs) {
            if(!typeInfo.isOrdered(expr.typ)) {
              ctx.unorderedOrderBy(expr.typ, expr.position.reference)
            }
          }

          // Ok, so this is a little subtle.  ORDER BY clauses must
          // come from the distinct list until that's exhausted...
          @tailrec
          def loop(distinctExprs: Set[Expr], remainingDistinctExprs: Set[Expr], orderBy: LazyList[Expr]): Unit = {
            if(remainingDistinctExprs.nonEmpty && orderBy.nonEmpty) {
              if(distinctExprs(orderBy.head)) {
                loop(distinctExprs, remainingDistinctExprs - orderBy.head, orderBy.tail)
              } else {
                ctx.distinctOnMustBePrefixOfOrderBy(orderBy.head.position.reference)
              }
            }
          }
          loop(exprs.to(Set), exprs.to(Set), checkedOrderBys.to(LazyList).map(_.expr))
        case Distinctiveness.FullyDistinct() =>
          for(missingOb <- checkedOrderBys.find { ob => !finalState.allNamedExprs.values.exists(_ == ob.expr) }) {
            ctx.orderByMustBeSelected(missingOb.expr.position.reference)
          }
        case Distinctiveness.Indistinct() =>
          // all well
      }

      val selectList = OrderedMap() ++ aliasAnalysis.expressions.keysIterator.map { cn =>
        val expr = finalState.allNamedExprs(cn)
        labelProvider.columnLabel() -> NamedExpr(expr, cn, ctx.outputColumnHints.get(cn), isSynthetic = false)
      }

      val stmt = addSystemColumnsIfDesired(Select(
        checkedDistinct,
        selectList,
        completeFrom.typecheckOnClauses(ctx, finalState.referencableNamedExprs),
        checkedWhere,
        checkedGroupBys,
        checkedHaving,
        checkedOrderBys,
        limit,
        offset,
        search,
        hints.map { h =>
          h match {
            case ast.Materialized(_) => SelectHint.Materialized
            case ast.NoRollup(_) => SelectHint.NoRollup
            case ast.NoChainMerge(_) => SelectHint.NoChainMerge
            case ast.CompoundRollup(_) => SelectHint.CompoundRollup
            case ast.RollupAtJoin(_) => SelectHint.RollupAtJoin
          }
        }.toSet
      ), attemptToPreserveSystemColumns = ctx.attemptToPreserveSystemColumns)

      verifyAggregatesAndWindowFunctions(ctx, stmt)

      if(ctx.hiddenColumns.isEmpty) {
        FromStatement(stmt, labelProvider.tableLabel(), ctx.scopedResourceName, None)
      } else {
        // ok, this is a little unpleasant, thanks to DISTINCT.
        stmt.distinctiveness match {
          case Distinctiveness.Indistinct() | Distinctiveness.On(_) =>
            // easy case - just filter the select-list
            val filteredStmt = stmt.copy(
              selectList = stmt.selectList.filter {
                case (_, NamedExpr(_, cn, _, _)) => !ctx.hiddenColumns(cn)
              }
            )
            FromStatement(filteredStmt, labelProvider.tableLabel(), ctx.scopedResourceName, None)
          case Distinctiveness.FullyDistinct() =>
            // This works because IF a DISTINCT statement has an
            // order by, those order by clauses MUST appear in the
            // select list.  So we just need to find those columns
            // and order by the relevant column-refs.  We'll
            // actually lift the ORDER BY (together with any
            // LIMIT/OFFSET) entirely into the superquery

            val (potentiallyUnorderedStatement, potentialLimit, potentialOffset) =
              (stmt.copy(orderBy = Nil, limit = None, offset = None), stmt.limit, stmt.offset)

            val unfilteredFrom = FromStatement(potentiallyUnorderedStatement, labelProvider.tableLabel(), ctx.scopedResourceName, None)
            val filteredStmt =
              Select(
                Distinctiveness.Indistinct(),
                stmt.selectList.flatMap { case (label, NamedExpr(expr, cn, hints, isSynthetic)) =>
                  if(ctx.hiddenColumns(cn)) {
                    None
                  } else {
                    Some(labelProvider.columnLabel() -> NamedExpr(VirtualColumn(unfilteredFrom.label, label, expr.typ)(AtomicPositionInfo.Synthetic), cn, hints, isSynthetic = isSynthetic))
                  }
                },
                unfilteredFrom,
                None,
                Nil,
                None,
                stmt.orderBy.map { ob =>
                  val (columnLabel, columnTyp) =
                    stmt.selectList.iterator.collect { case (label, NamedExpr(expr, cn, hints, isSynthetic)) if expr == ob.expr =>
                      (label, expr.typ)
                    }.nextOption().getOrElse {
                      throw new Exception("Internal error: found an order by whose expr is not in the select list??")
                    }

                  ob.copy(expr = VirtualColumn(unfilteredFrom.label, columnLabel, columnTyp)(AtomicPositionInfo.Synthetic))
                },
                limit = potentialLimit,
                offset = potentialOffset,
                search = None,
                hint = Set.empty
              )
            FromStatement(filteredStmt, labelProvider.tableLabel(), None, None)
        }
      }
    }

    def collectNamesForAnalysis(from: FakeFrom): AliasAnalysis.AnalysisContext = {
      def contextFrom(af: AtomicFrom): UntypedDatasetContext =
        af match {
          case _: FromSingleRow => new UntypedDatasetContext {
            val columns = OrderedSet.empty
          }
          case t: FromTable => new UntypedDatasetContext {
            val columns = OrderedSet() ++ t.columns.valuesIterator.map(_.name)
          }
          case s: FromStatement => new UntypedDatasetContext {
            val columns = OrderedSet() ++ s.statement.schema.valuesIterator.map(_.name)
          }
        }

      def augmentAcc(acc: Map[AliasAnalysis.Qualifier, UntypedDatasetContext], from: AtomicFrom) = {
        from.alias match {
          case None =>
            acc + (TableName.PrimaryTable.qualifier -> contextFrom(from))
          case Some(alias) =>
            val acc1 =
              if(acc.isEmpty) {
                acc + (TableName.PrimaryTable.qualifier -> contextFrom(from))
              } else {
                acc
              }
            acc1 + (TableName.SodaFountainPrefix + alias.name -> contextFrom(from))
        }
      }

      from.reduce[Map[AliasAnalysis.Qualifier, UntypedDatasetContext]](
        augmentAcc(Map.empty, _),
        { (acc, right) => augmentAcc(acc, right) }
      )
    }

    sealed abstract class FakeFrom {
      // This is a real From where the ON clauses are all `true`
      def intoFakeRealFrom: From

      final def extendEnvironment(base: Environment[MT]): Either[AddScopeError, Environment[MT]] =
        intoFakeRealFrom.extendEnvironment(base)
      final def typecheckOnClauses(ctx: Ctx, selectList: Map[ColumnName, Expr]): From =
        doTypecheckOnClauses(ctx, selectList)._1

      def doTypecheckOnClauses(ctx: Ctx, availableSelectList: Map[ColumnName, Expr]): (From, Environment[MT])

      final def reduce[S](
        base: AtomicFrom => S,
        combine: (S, AtomicFrom) => S
      ): S = intoFakeRealFrom.reduce[S](base, { (s, join) => combine(s, join.right) })
    }
    case class FakeAtomicFrom(atomicFrom: AtomicFrom) extends FakeFrom {
      override def intoFakeRealFrom = atomicFrom

      override def doTypecheckOnClauses(ctx: Ctx, availableSelectList: Map[ColumnName, Expr]) =
        // This extendEnvironment should not fail, as we already did
        // this extension once in queryInputSchema.
        (atomicFrom, envify(ctx, atomicFrom.extendEnvironment(ctx.enclosingEnv), NoPosition /* TODO: NEED POS INFO FROM AST */))
    }
    case class FakeJoin(joinType: JoinType, lateral: Boolean, left: FakeFrom, right: AtomicFrom, on: ast.Expression) extends FakeFrom {
      override val intoFakeRealFrom =
        Join(joinType, lateral, left.intoFakeRealFrom, right, typeInfo.literalBoolean(true, Source.Synthetic))

      override def doTypecheckOnClauses(ctx: Ctx, availableSelectList: Map[ColumnName, Expr]) = {

        // for things to the left of us, remove any columns that
        // reference the right side of this JOIN.
        val nextSelectList: Map[ColumnName, Expr] =
          availableSelectList.filterNot { case (_, e) =>
            e.columnReferences.contains(right.label)
          }

        val (realLeft, env0) = left.doTypecheckOnClauses(ctx, nextSelectList)

        // Because we're re-building the unitary join environment, we
        // want to add a scope to th existing environment rather than
        // adding a new sub-environment here.  It has already been
        // extended by the leftmost part of the join.
        val env = envify(ctx, right.addToEnvironment(env0), NoPosition /* TODO: NEED POS INFO FROM AST */)

        val checkedOn =
          typecheck(ctx.copy(enclosingEnv = env), on, availableSelectList, Some(typeInfo.boolType))

        if(!typeInfo.isBoolean(checkedOn.typ)) {
          ctx.expectedBoolean(on, checkedOn.typ)
        }

        (Join(joinType, lateral, realLeft, right, checkedOn), env)
      }
    }


    def queryInputSchema(ctx: Ctx, input: ImplicitFrom, from: Option[TableName], joins: Seq[ast.Join]): FakeFrom = {
      // Ok so:
      //  * @This is special only in From
      //  * It is an error if we have neither an input nor a from
      //  * It is an error if we have an input and a non-@This from

      // The AST and this analyzer have a fundamentally different idea
      // of what joins are.  The AST sees it as a table + zero or more
      // join clauses.  This analyzer sees it as a non-empty linked
      // list of tables where the links are decorated with the details
      // of the join.  In addition, to typecheck LATERAL joins, we
      // need the from-clause-so-far to be introspectable.
      //
      // So, what we're actually doing is scanning the input from
      // left to right, building up functions ("pendingJoins") which
      // will afterward be used to build up the linked list that this
      // analyzer wants from right to left.

      def fromNamedTable(rn: ResourceName, alias: ResourceName) = {
        analyzeForFrom(ctx.scopedResourceName, ScopedResourceName(ctx.scope, rn), ctx.canonicalName, NoPosition /* TODO: NEED POS INFO FROM AST */).
          reAlias(Some(alias))
      }

      val from0: AtomicFrom = (input, from) match {
        case (ImplicitFrom.None, None) =>
          // No required context and no from given; this is an error
          ctx.noDataSource(NoPosition /* TODO: NEED POS INFO FROM AST */)
        case (ImplicitFrom.Required(prev), Some(tn)) =>
          tn.aliasWithoutPrefix.foreach(ensureLegalAlias(ctx, _, NoPosition /* TODO: NEED POS INFO FROM AST */))
          val rn = ResourceName(tn.nameWithoutPrefix)
          if(rn == SoQLAnalyzer.This) {
            // chained query: {something} |> select ... from @this [as alias]
            prev.reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
          } else {
            // chained query: {something} |> select ... from somethingThatIsNotThis
            // this is an error
            ctx.chainWithFrom(NoPosition /* TODO: NEED POS INFO FROM AST */)
          }
        case (optionalPrev: ImplicitFrom.Optional, Some(tn)) =>
          tn.aliasWithoutPrefix.foreach(ensureLegalAlias(ctx, _, NoPosition /* TODO: NEED POS INFO FROM AST */))
          val rn = ResourceName(tn.nameWithoutPrefix)
          val alias = ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))
          if(rn == SoQLAnalyzer.This) {
            // chained query: {something} |> select ... from @this [as alias]
            optionalPrev.get.reAlias(Some(alias))
          } else {
            // we have an implicit from, but we're not required to use it
            // so we've got "select ... from sometable ..." here.
            // n.b., sometable may actually be a query
            fromNamedTable(rn, alias)
          }
        case (ImplicitFrom.None, Some(tn)) =>
          tn.aliasWithoutPrefix.foreach(ensureLegalAlias(ctx, _, NoPosition /* TODO: NEED POS INFO FROM AST */))
          val rn = ResourceName(tn.nameWithoutPrefix)
          val alias = ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))
          if(rn == SoQLAnalyzer.This) {
            // standalone query: select ... from @this.  But it's standalone, so this is an error
            ctx.fromThisWithoutContext(NoPosition /* TODO: NEED POS INFO FROM AST */)
          } else {
            // standalone query: select ... from sometable ...
            // n.b., sometable may actually be a query
            fromNamedTable(rn, alias)
          }
        case (ImplicitFrom.Required(input), None) =>
          // chained query: {something} |> {the thing we're analyzing}
          input.reAlias(None)
        case (opt: ImplicitFrom.Optional, None) =>
          if(opt.leftmost || ImplicitFrom.allowNonLeftmostImplicitFromInTableOp) {
            // chained query: {something} |> {the thing we're analyzing}
            opt.get.reAlias(None)
          } else {
            ctx.noDataSource(NoPosition /* TODO: NEED POS INFO FROM AST */)
          }
      }

      joins.foldLeft[FakeFrom](FakeAtomicFrom(from0)) { (left, join) =>
        val joinType = join.typ match {
          case ast.InnerJoinType => JoinType.Inner
          case ast.LeftOuterJoinType => JoinType.LeftOuter
          case ast.RightOuterJoinType => JoinType.RightOuter
          case ast.FullOuterJoinType => JoinType.FullOuter
        }

        val augmentedFrom = envify(ctx, left.extendEnvironment(ctx.enclosingEnv), NoPosition /* TODO: NEED POS INFO FROM AST */)
        val effectiveLateral = (join.lateral && !definitelyDoesNotRequireLateral(join)) || requiresLateral(join)
        val checkedRight =
          if(effectiveLateral) {
            analyzeJoinSelect(ctx.copy(enclosingEnv = augmentedFrom), join.from)
          } else {
            analyzeJoinSelect(ctx, join.from)
          }

        FakeJoin(joinType, effectiveLateral, left, checkedRight, join.on)
      }
    }

    def requiresLateral(join: ast.Join): Boolean =
      join.from match {
        case ast.JoinFunc(_, params) => params.exists(containsColumnRef)
        case _ => false
      }

    def definitelyDoesNotRequireLateral(join: ast.Join): Boolean =
      join.from match {
        case ast.JoinFunc(_, _) => !requiresLateral(join)
        case _ => false
      }

    def containsColumnRef(expr: ast.Expression): Boolean =
      expr match {
        case _ : ast.ColumnOrAliasRef => true
        case _ : ast.Literal => false
        case _ : ast.Hole.UDF => true // `?x` - becomes a column ref
        case _ : ast.Hole.SavedQuery => false // `param` call - becomes a literal
        case ast.FunctionCall(_, params, filter, windowFunctionInfo) =>
          params.exists(containsColumnRef) ||
            filter.exists(containsColumnRef) ||
            windowFunctionInfo.fold(false) { case ast.WindowFunctionInfo(partitions, orderings, frames) =>
              partitions.exists(containsColumnRef) ||
                orderings.exists { ob => containsColumnRef(ob.expression) }
            }
      }

    def envify[T](ctx: Ctx, result: Either[AddScopeError, T], pos: Position): T =
      result match {
        case Right(r) => r
        case Left(e) => ctx.addScopeError(e, pos)
      }


    def analyzeJoinSelect(ctx: Ctx, js: ast.JoinSelect): AtomicFrom = {
      js match {
        case ast.JoinTable(tn) =>
          tn.aliasWithoutPrefix.foreach(ensureLegalAlias(ctx, _, NoPosition /* TODO: NEED POS INFO FROM AST */))
          analyzeForFrom(ctx.scopedResourceName, ScopedResourceName(ctx.scope, ResourceName(tn.nameWithoutPrefix)), ctx.canonicalName, NoPosition /* TODO: NEED POS INFO FROM AST */).
            reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
        case ast.JoinQuery(select, rawAlias) =>
          val alias = rawAlias.substring(1)
          ensureLegalAlias(ctx, alias, NoPosition /* TODO: NEED POS INFO FROM AST */)
          val subCtx = Ctx(
            scope = ctx.scope,
            scopedResourceName = ctx.scopedResourceName, // still in the same source-code context...
            canonicalName = ctx.canonicalName,
            primaryTableName = primaryTableName(ctx.scope, select),
            enclosingEnv = ctx.enclosingEnv,
            udfParams = ctx.udfParams,
            hiddenColumns = Set.empty,
            outputColumnHints = Map.empty,
            attemptToPreserveSystemColumns = preserveSystemColumnsRequested
          )
          analyzeStatement(subCtx, select, ImplicitFrom.None).reAlias(Some(ResourceName(alias)))
        case ast.JoinFunc(tn, params) =>
          tn.aliasWithoutPrefix.foreach(ensureLegalAlias(ctx, _, NoPosition /* TODO: NEED POS INFO FROM AST */))
          analyzeUDF(ctx, ResourceName(tn.nameWithoutPrefix), params).
            reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
      }
    }

    def ensureLegalAlias(ctx: Ctx, candidate: String, position: Position): Unit = {
      val rnCandidate = ResourceName(candidate)
      if(SoQLAnalyzer.SpecialNames(rnCandidate)) {
        ctx.reservedTableName(rnCandidate, position)
      }
    }

    def analyzeUDF(callerCtx: Ctx, resource: ResourceName, params: Seq[ast.Expression]): AtomicFrom = {
      val udfScopedResourceName = ScopedResourceName(callerCtx.scope, resource)
      tableMap.find(udfScopedResourceName) match {
        case TableDescription.TableFunction(udfScope, udfCanonicalName, parsed, _unparsed, paramSpecs, hiddenColumns) =>
          if(params.length != paramSpecs.size) {
            callerCtx.incorrectNumberOfParameters(resource, expected = params.length, got = paramSpecs.size, position = NoPosition /* TODO: NEED POS INFO FROM AST */)
          }
          // we're rewriting the UDF from
          //    @bleh(x, y, z)
          // to
          //    select * from (values (x,y,z)) join lateral (udfexpansion) on true
          // Possibly we'll want a rewrite pass that inlines constants and
          // maybe simple column references into the udf expansion,

          val typecheckedParams =
            OrderedMap() ++ params.lazyZip(paramSpecs).map { case (expr, (name, typ)) =>
              val rewrittenExpr =
                if(allowSloppyUDFParams) {
                  ast.FunctionCall(ast.SpecialFunctions.Cast(typeInfo.typeNameFor(typ)), Seq(expr))(expr.position, expr.position)
                } else {
                  expr
                }

              name -> typecheck(callerCtx, rewrittenExpr, Map.empty, Some(typ))
            }

          NonEmptySeq.fromSeq(typecheckedParams.toVector) match {
            case Some(typecheckedNamesAndExprs) =>
              val outOfLineParamsQuery = Select(
                Distinctiveness.Indistinct(),
                OrderedMap() ++ typecheckedNamesAndExprs.iterator.map { case (holeName, expr) => labelProvider.columnLabel() -> NamedExpr(expr, ColumnName(holeName.name), hint = None, isSynthetic = true) },
                FromSingleRow(labelProvider.tableLabel(), None),
                None, Nil, None, Nil, None, None, None,
                Set.empty
              )
              val outOfLineParamsLabel = labelProvider.tableLabel()
              val innerUdfParams =
                outOfLineParamsQuery.schema.keys.lazyZip(typecheckedParams).map { case (colLabel, (name, expr)) =>
                  name -> { (sourceName: Option[ScopedResourceName], p: Position) => VirtualColumn(outOfLineParamsLabel, colLabel, expr.typ)(new AtomicPositionInfo(Source.nonSynthetic(sourceName, p))) }
                }.toMap

              val udfCtx = Ctx(
                scope = udfScope,
                scopedResourceName = Some(udfScopedResourceName),
                canonicalName = Some(udfCanonicalName),
                primaryTableName = primaryTableName(udfScope, parsed),
                enclosingEnv = Environment.empty,
                udfParams = innerUdfParams,
                hiddenColumns = hiddenColumns,
                outputColumnHints = Map.empty,
                attemptToPreserveSystemColumns = callerCtx.attemptToPreserveSystemColumns
              )

              val useQuery = analyzeStatement(udfCtx, parsed, ImplicitFrom.None)

              FromStatement(
                Select(
                  Distinctiveness.Indistinct(),
                  OrderedMap() ++ useQuery.statement.schema.iterator.map { case (label, Statement.SchemaEntry(name, typ, hint, isSynthetic)) =>
                    labelProvider.columnLabel() -> NamedExpr(VirtualColumn[MT](useQuery.label, label, typ)(AtomicPositionInfo.Synthetic), name, None, isSynthetic = isSynthetic)
                  },
                  Join(
                    JoinType.Inner,
                    true,
                    FromStatement(outOfLineParamsQuery, outOfLineParamsLabel, Some(udfScopedResourceName), None),
                    useQuery,
                    typeInfo.literalBoolean(true, Source.Synthetic)
                  ),
                  None,
                  Nil,
                  None,
                  Nil,
                  None,
                  None,
                  None,
                  Set.empty
                ),
                labelProvider.tableLabel(),
                Some(udfScopedResourceName),
                None
              )

            case None =>
              // There are no parameters, so we can just
              // expand the UDF without introducing a layer of
              // additional joining.
              val udfCtx = Ctx(
                scope = udfScope,
                scopedResourceName = Some(udfScopedResourceName),
                canonicalName = Some(udfCanonicalName),
                primaryTableName = primaryTableName(udfScope, parsed),
                enclosingEnv = Environment.empty,
                udfParams = Map.empty,
                hiddenColumns = hiddenColumns,
                outputColumnHints = Map.empty,
                attemptToPreserveSystemColumns = callerCtx.attemptToPreserveSystemColumns
              )

              analyzeStatement(udfCtx, parsed, ImplicitFrom.None)
          }
        case _ =>
          // Non-UDF
          callerCtx.parametersForNonUdf(resource, position = NoPosition /* TODO: NEED POS INFO FROM AST */)
      }
    }

    def typecheck(
      ctx: Ctx,
      expr: ast.Expression,
      namedExprs: Map[ColumnName, Expr],
      expectedType: Option[CT]
    ): Expr = {
      val tc = new Typechecker(ctx.scopedResourceName, ctx.canonicalName, ctx.primaryTableName, ctx.enclosingEnv, namedExprs, ctx.udfParams, userParameters, typeInfo, functionInfo)
      tc(expr, expectedType) match {
        case Right(e) => e
        case Left(err) => augmentTypecheckException(err)
      }
    }

    def verifyAggregatesAndWindowFunctions(ctx: Ctx, stmt: Select): Unit = {
      val isAggregated = stmt.isAggregated
      val groupBys = stmt.groupBy.toSet

      val preGroupCtx = VerifyCtx(ctx, allowAggregates = false, allowWindow = false, groupBys = Set.empty)
      val havingCtx = VerifyCtx(ctx, allowAggregates = true, allowWindow = false, groupBys = groupBys)
      val postHavingCtx = VerifyCtx(ctx, allowAggregates = isAggregated, allowWindow = true, groupBys = groupBys)

      stmt.from.reduce[Unit](
        { _ => () },
        { (_, join) => verifyAggregatesAndWindowFunctions(preGroupCtx, join.on) }
      )
      for(w <- stmt.where) verifyAggregatesAndWindowFunctions(preGroupCtx, w)
      for(gb <- stmt.groupBy) verifyAggregatesAndWindowFunctions(preGroupCtx, gb)
      for(h <- stmt.having) verifyAggregatesAndWindowFunctions(havingCtx, h)
      for(ob <- stmt.orderBy) verifyAggregatesAndWindowFunctions(postHavingCtx, ob.expr)
      for((_, s) <- stmt.selectList) verifyAggregatesAndWindowFunctions(postHavingCtx, s.expr)
    }
    case class VerifyCtx(ctx: Ctx, allowAggregates: Boolean, allowWindow: Boolean, groupBys: Set[Expr])
    def verifyAggregatesAndWindowFunctions(ctx: VerifyCtx, e: Expr): Unit = {
      e match {
        case _ : Literal =>
        // literals are always ok
        case AggregateFunctionCall(f, args, _distinct, filter) if ctx.allowAggregates =>
          val subCtx = ctx.copy(allowAggregates = false, allowWindow = false)
          args.foreach(verifyAggregatesAndWindowFunctions(subCtx, _))
        case agg: AggregateFunctionCall =>
          ctx.ctx.aggregateFunctionNotAllowed(agg.function.name, agg.position.functionNameSource)
        case w@WindowedFunctionCall(f, args, filter, partitionBy, orderBy, _frame) if ctx.allowWindow =>
          // Fun fact: this order by does not have the same no-literals restriction as a select's order-by
          val subCtx = ctx.copy(allowAggregates = ctx.allowAggregates && w.isAggregated, allowWindow = false)
          args.foreach(verifyAggregatesAndWindowFunctions(subCtx, _))
          partitionBy.foreach(verifyAggregatesAndWindowFunctions(subCtx, _))
          orderBy.foreach { ob => verifyAggregatesAndWindowFunctions(subCtx, ob.expr) }
        case wfc: WindowedFunctionCall =>
          ctx.ctx.windowFunctionNotAllowed(wfc.function.name, wfc.position.functionNameSource)
        case e: Expr if ctx.allowAggregates && ctx.groupBys.contains(e) =>
          // ok, we want an aggregate and this is an expression from the GROUP BY clause
        case FunctionCall(_f, args) =>
          // This is a valid aggregate if all our arguments are valid aggregates
          args.foreach(verifyAggregatesAndWindowFunctions(ctx, _))
        case c: Column if ctx.allowAggregates =>
          // Column reference, but it's not from the group by clause.
          // Fail!
          ctx.ctx.ungroupedColumnReference(c.position.reference)
        case _ =>
          // ok
      }
    }

    def augmentTypecheckException(tce: SoQLAnalyzerError.TypecheckError[RNS]): Nothing =
      throw Bail(tce)
  }

  def illegalThisReference(source: Option[ScopedResourceName], position: Position): Nothing =
    throw Bail(Error.IllegalThisReference(Source.nonSynthetic(source, position)))
  def parameterlessTableFunction(source: Option[ScopedResourceName], name: ResourceName, position: Position): Nothing =
    throw Bail(Error.ParameterlessTableFunction(Source.nonSynthetic(source, position), name))
}
