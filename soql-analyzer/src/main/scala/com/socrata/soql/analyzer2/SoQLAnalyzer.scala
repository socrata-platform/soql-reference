// TODO:
//  * Errors
//     - everything needs position info
//  * More postprocessing passes in the SoQLAnalysis
//     - provide_order(using_column: DatabaseTableName => DatabaseColumnName)

package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.control.NoStackTrace
import scala.util.parsing.input.{Position, NoPosition}
import scala.collection.compat._

import com.socrata.soql.{BinaryTree, Leaf, TrueOp, PipeQuery, UnionQuery, UnionAllQuery, IntersectQuery, IntersectAllQuery, MinusQuery, MinusAllQuery}
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.ast
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName, HoleName, UntypedDatasetContext, FunctionName}
import com.socrata.soql.typechecker.{TypeInfo2, FunctionInfo, HasType}
import com.socrata.soql.aliases.AliasAnalysis
import com.socrata.soql.exceptions.AliasAnalysisException

object SoQLAnalyzer {
  private val This = ResourceName("this")
  private val SingleRow = ResourceName("single_row")

  private val SpecialNames = Set(This, SingleRow)
}

class SoQLAnalyzer[RNS, CT, CV](typeInfo: TypeInfo2[CT, CV], functionInfo: FunctionInfo[CT]) {
  type ScopedResourceName = com.socrata.soql.analyzer2.ScopedResourceName[RNS]
  type TableMap = com.socrata.soql.analyzer2.TableMap[RNS, CT]
  type FoundTables = com.socrata.soql.analyzer2.FoundTables[RNS, CT]
  type TableDescription = com.socrata.soql.analyzer2.TableDescription[RNS, CT]
  type UserParameters = com.socrata.soql.analyzer2.UserParameters[CT, CV]
  type UserParameterSpecs = com.socrata.soql.analyzer2.UserParameterSpecs[CT]

  type UdfParameters = Map[HoleName, Position => Expr[CT, CV]]

  private case class Bail(result: SoQLAnalyzerError[RNS]) extends Exception with NoStackTrace

  def apply(start: FoundTables, userParameters: UserParameters): Either[SoQLAnalyzerError[RNS], SoQLAnalysis[RNS, CT, CV]] = {
    try {
      validateUserParameters(start.knownUserParameters, userParameters)

      val state = new State(start.tableMap, userParameters)

      Right(new SoQLAnalysis[RNS, CT, CV](
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

  private class State(tableMap: TableMap, userParameters: UserParameters) {
    val labelProvider = new LabelProvider

    def analyze(scope: RNS, query: FoundTables.Query[CT]): Statement[RNS, CT, CV] = {
      val from =
        query match {
          case FoundTables.Saved(rn) =>
            analyzeForFrom(ScopedResourceName(scope, rn), None, NoPosition)
          case FoundTables.InContext(rn, q, _, parameters) =>
            val from = analyzeForFrom(ScopedResourceName(scope, rn), None, NoPosition)
            new Context(scope, None, Environment.empty, Map.empty).
              analyzeStatement(None, q, Some(from))
          case FoundTables.InContextImpersonatingSaved(rn, q, _, parameters, impersonating) =>
            val from = analyzeForFrom(ScopedResourceName(scope, rn), None, NoPosition)
            new Context(scope, Some(impersonating), Environment.empty, Map.empty).
              analyzeStatement(None, q, Some(from))
          case FoundTables.Standalone(q, _, parameters) =>
            new Context(scope, None, Environment.empty, Map.empty).
              analyzeStatement(None, q, None)
        }
      intoStatement(from)
    }

    def intoStatement(from: AtomicFrom[RNS, CT, CV]): Statement[RNS, CT, CV] = {
      from match {
        case from: FromTable[RNS, CT] =>
          selectFromFrom(
            from.columns.map { case (label, NameEntry(name, typ)) =>
              labelProvider.columnLabel() -> NamedExpr(Column(from.label, label, typ)(AtomicPositionInfo.None), name)
            },
            from
          )
        case from: FromSingleRow[RNS] =>
          selectFromFrom(OrderedMap.empty[AutoColumnLabel, NamedExpr[CT, CV]], from)
        case from: FromStatement[RNS, CT, CV] =>
          // Just short-circuit it and return the underlying Statement
          from.statement
      }
    }

    def selectFromFrom(
      selectList: OrderedMap[AutoColumnLabel, NamedExpr[CT, CV]],
      from: AtomicFrom[RNS, CT, CV]
    ): Statement[RNS, CT, CV] = {
      Select(
        distinctiveness = Distinctiveness.Indistinct,
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

    def fromTable(srn: ScopedResourceName, desc: TableDescription.Dataset[CT]): AtomicFrom[RNS, CT, Nothing] = {
      if(desc.ordering.isEmpty) {
        FromTable(
          desc.name,
          srn,
          None,
          labelProvider.tableLabel(),
          columns = desc.schema
        )
      } else {
        for(TableDescription.Ordering(col, _ascending) <- desc.ordering) {
          val typ = desc.schema(col).typ
          if(!typeInfo.isOrdered(typ)) {
            throw Bail(SoQLAnalyzerError.UnorderedOrderBy(srn.scope, Some(desc.canonicalName), typeInfo.typeNameFor(typ), NoPosition))
          }
        }

        val from = FromTable(
          desc.name,
          srn,
          None,
          labelProvider.tableLabel(),
          columns = desc.schema
        )

        val columnLabels = from.columns.map { case _ => labelProvider.columnLabel() }

        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            selectList = OrderedMap() ++ from.columns.iterator.zip(columnLabels.iterator).map { case ((dcn, NameEntry(name, typ)), outputLabel) =>
              outputLabel -> NamedExpr(Column(from.label, dcn, typ)(AtomicPositionInfo.None), name)
            },
            from,
            None,
            Nil,
            None,
            desc.ordering.map { case TableDescription.Ordering(dcn, ascending) =>
              val NameEntry(_, typ) = from.columns(dcn)
              OrderBy(Column(from.label, dcn, typ)(AtomicPositionInfo.None), ascending = ascending, nullLast = ascending)
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

    def analyzeForFrom(name: ScopedResourceName, canonicalName: Option[CanonicalName], position: Position): AtomicFrom[RNS, CT, CV] = {
      if(name.name == SoQLAnalyzer.SingleRow) {
        FromSingleRow(labelProvider.tableLabel(), None)
      } else if(name.name == SoQLAnalyzer.This) {
        // analyzeSelection will handle @this in the correct
        // position...
        illegalThisReference(name.scope, canonicalName, position)
      } else {
        tableMap.find(name) match {
          case ds: TableDescription.Dataset[CT] =>
            fromTable(name, ds)
          case TableDescription.Query(scope, canonicalName, basedOn, parsed, _unparsed, parameters) =>
            // so this is basedOn |> parsed
            // so we want to use "basedOn" as the implicit "from" for "parsed"
            val from = analyzeForFrom(ScopedResourceName(scope, basedOn), None, NoPosition /* Yes, actually NoPosition here */)
            new Context(scope, Some(canonicalName), Environment.empty, Map.empty).
              analyzeStatement(Some(name), parsed, Some(from))
          case TableDescription.TableFunction(_, _, _, _, _) =>
            parameterlessTableFunction(name.scope, canonicalName, name.name, position)
        }
      }
    }

    class Context(scope: RNS, canonicalName: Option[CanonicalName], enclosingEnv: Environment[CT], udfParams: UdfParameters) {
      private def withEnv(env: Environment[CT]) = new Context(scope, canonicalName, env, udfParams)

      def analyzeStatement(srn: Option[ScopedResourceName], q: BinaryTree[ast.Select], from0: Option[AtomicFrom[RNS, CT, CV]]): FromStatement[RNS, CT, CV] = {
        q match {
          case Leaf(select) =>
            analyzeSelection(srn, select, from0)
          case PipeQuery(left, right) =>
            val newInput = analyzeStatement(None, left, from0)
            analyzeStatement(srn, right, Some(newInput))
          case other: TrueOp[ast.Select] =>
            val lhs = intoStatement(analyzeStatement(None, other.left, from0))
            val rhs = intoStatement(analyzeStatement(None, other.right, None))

            if(lhs.schema.values.map(_.typ) != rhs.schema.values.map(_.typ)) {
              tableOpTypeMismatch(
                OrderedMap() ++ lhs.schema.valuesIterator.map { case NameEntry(name, typ) => name -> typ },
                OrderedMap() ++ rhs.schema.valuesIterator.map { case NameEntry(name, typ) => name -> typ },
                NoPosition /* TODO: NEED POS INFO FROM AST */
              )
            }

            FromStatement(CombinedTables(opify(other), lhs, rhs), labelProvider.tableLabel(), srn, None)
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

      def analyzeSelection(srn: Option[ScopedResourceName], select: ast.Select, from0: Option[AtomicFrom[RNS, CT, CV]]): FromStatement[RNS, CT, CV] = {
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
        val completeFrom = queryInputSchema(from0, from, joins)
        val localEnv = envify(completeFrom.extendEnvironment(enclosingEnv), NoPosition /* TODO: NEED POS INFO FROM AST */)

        // Now that we know what we're selecting from, we'll give names to the selection...
        val aliasAnalysis =
          try {
            AliasAnalysis(selection, from)(collectNamesForAnalysis(completeFrom))
          } catch {
            case e: AliasAnalysisException =>
              augmentAliasAnalysisException(e)
          }

        // With the aliases assigned we'll typecheck the select-list,
        // building up the set of named exprs we can use as shortcuts
        // for other expressions
        class EvaluationState(val namedExprs: Map[ColumnName, Expr[CT, CV]] = OrderedMap.empty) {
          def update(name: ColumnName, expr: Expr[CT, CV]): EvaluationState = {
            new EvaluationState(namedExprs + (name -> expr))
          }

          def typecheck(expr: ast.Expression, expectedType: Option[CT] = None) =
            withEnv(localEnv).typecheck(expr, namedExprs, None)
        }

        val finalState = aliasAnalysis.evaluationOrder.foldLeft(new EvaluationState()) { (state, colName) =>
          val expression = aliasAnalysis.expressions(colName)
          val typed = state.typecheck(expression)
          state.update(colName, typed)
        }

        val checkedDistinct = distinct match {
          case ast.Indistinct => Distinctiveness.Indistinct
          case ast.FullyDistinct => Distinctiveness.FullyDistinct
          case ast.DistinctOn(exprs) =>
            Distinctiveness.On(
              exprs.map { expr =>
                if(expr.isInstanceOf[ast.Literal]) {
                  literalNotAllowedInDistinctOn(expr.position)
                }
                finalState.typecheck(expr)
              }
            )
        }

        val checkedWhere = where.map(finalState.typecheck(_, Some(typeInfo.boolType)))
        val checkedGroupBys = groupBys.map { expr =>
          if(expr.isInstanceOf[ast.Literal]) {
            literalNotAllowedInGroupBy(expr.position)
          }
          val checked = finalState.typecheck(expr)
          if(!typeInfo.isGroupable(checked.typ)) {
            invalidGroupBy(checked.typ, checked.position.logicalPosition)
          }
          checked
        }
        val checkedHaving = having.map(finalState.typecheck(_, Some(typeInfo.boolType)))
        val checkedOrderBys = orderBys.map { case ast.OrderBy(expr, ascending, nullLast) =>
          if(expr.isInstanceOf[ast.Literal]) {
            literalNotAllowedInOrderBy(expr.position)
          }
          val checked = finalState.typecheck(expr)
          if(!typeInfo.isOrdered(checked.typ)) {
            unorderedOrderBy(checked.typ, checked.position.logicalPosition)
          }
          OrderBy(checked, ascending, nullLast)
        }

        checkedDistinct match {
          case Distinctiveness.On(exprs) =>
            if(checkedOrderBys.nonEmpty){
              if(!checkedOrderBys.map(_.expr).startsWith(exprs)) {
                distinctOnMustBePrefixOfOrderBy(NoPosition /* TODO: NEED POS INFO FROM AST */)
              }
            } else { // distinct on without an order by implicitly orders by the distinct columns
              for(expr <- exprs) {
                if(!typeInfo.isOrdered(expr.typ)) {
                  unorderedOrderBy(expr.typ, expr.position.logicalPosition)
                }
              }
            }
          case Distinctiveness.FullyDistinct =>
            for(missingOb <- checkedOrderBys.find { ob => !finalState.namedExprs.values.exists(_ == ob.expr) }) {
              orderByMustBeSelected(missingOb.expr.position.logicalPosition)
            }
          case Distinctiveness.Indistinct =>
            // all well
        }

        val stmt = Select(
          checkedDistinct,
          OrderedMap() ++ aliasAnalysis.expressions.keysIterator.map { cn =>
            val expr = finalState.namedExprs(cn)
            labelProvider.columnLabel() -> NamedExpr(expr, cn)
          },
          completeFrom,
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
        )

        verifyAggregatesAndWindowFunctions(stmt)

        FromStatement(stmt, labelProvider.tableLabel(), srn, None)
      }

      def collectNamesForAnalysis(from: From[RNS, CT, CV]): AliasAnalysis.AnalysisContext = {
        def contextFrom(af: AtomicFrom[RNS, CT, CV]): UntypedDatasetContext =
          af match {
            case _: FromSingleRow[RNS] => new UntypedDatasetContext {
              val columns = OrderedSet.empty
            }
            case t: FromTable[RNS, CT] => new UntypedDatasetContext {
              val columns = OrderedSet() ++ t.columns.valuesIterator.map(_.name)
            }
            case s: FromStatement[RNS, CT, CV] => new UntypedDatasetContext {
              val columns = OrderedSet() ++ s.statement.schema.valuesIterator.map(_.name)
            }
          }

        def augmentAcc(acc: Map[AliasAnalysis.Qualifier, UntypedDatasetContext], from: AtomicFrom[RNS, CT, CV]) = {
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
          { (acc, j) => augmentAcc(acc, j.right) }
        )
      }

      def queryInputSchema(input: Option[AtomicFrom[RNS, CT, CV]], from: Option[TableName], joins: Seq[ast.Join]): From[RNS, CT, CV] = {
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

        val from0: AtomicFrom[RNS, CT, CV] = (input, from) match {
          case (None, None) =>
            // No context and no from; this is an error
            noDataSource(NoPosition /* TODO: NEED POS INFO FROM AST */)
          case (Some(prev), Some(tn)) =>
            tn.aliasWithoutPrefix.foreach(ensureLegalAlias(_, NoPosition /* TODO: NEED POS INFO FROM AST */))
            val rn = ResourceName(tn.nameWithoutPrefix)
            if(rn == SoQLAnalyzer.This) {
              // chained query: {something} |> select ... from @this [as alias]
              prev.reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
            } else {
              // chained query: {something} |> select ... from somethingThatIsNotThis
              // this is an error
              chainWithFrom(NoPosition /* TODO: NEED POS INFO FROM AST */)
            }
          case (None, Some(tn)) =>
            tn.aliasWithoutPrefix.foreach(ensureLegalAlias(_, NoPosition /* TODO: NEED POS INFO FROM AST */))
            val rn = ResourceName(tn.nameWithoutPrefix)
            val alias = ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))
            if(rn == SoQLAnalyzer.This) {
              // standalone query: select ... from @this.  But it's standalone, so this is an error
              fromThisWithoutContext(NoPosition /* TODO: NEED POS INFO FROM AST */)
            } else {
              // standalone query: select ... from sometable ...
              // n.b., sometable may actually be a query
              analyzeForFrom(ScopedResourceName(scope, rn), canonicalName, NoPosition /* TODO: NEED POS INFO FROM AST */).
                reAlias(Some(alias))
            }
          case (Some(input), None) =>
            // chained query: {something} |> {the thing we're analyzing}
            input.reAlias(None)
        }

        joins.foldLeft[From[RNS, CT, CV]](from0) { (left, join) =>
          val joinType = join.typ match {
            case ast.InnerJoinType => JoinType.Inner
            case ast.LeftOuterJoinType => JoinType.LeftOuter
            case ast.RightOuterJoinType => JoinType.RightOuter
            case ast.FullOuterJoinType => JoinType.FullOuter
          }

          val augmentedFrom = envify(left.extendEnvironment(enclosingEnv), NoPosition /* TODO: NEED POS INFO FROM AST */)
          val effectiveLateral = join.lateral || join.from.isInstanceOf[ast.JoinFunc]
          val checkedRight =
            if(effectiveLateral) {
              withEnv(augmentedFrom).analyzeJoinSelect(join.from)
            } else {
              analyzeJoinSelect(join.from)
            }

          val checkedOn = withEnv(envify(checkedRight.addToEnvironment(augmentedFrom), NoPosition /* TODO: NEED POS INFO FROM AST */)).
            typecheck(join.on, Map.empty, Some(typeInfo.boolType))

          if(!typeInfo.isBoolean(checkedOn.typ)) {
            expectedBoolean(join.on, checkedOn.typ)
          }

          Join(joinType, effectiveLateral, left, checkedRight, checkedOn)
        }
      }

      def envify[T](result: Either[AddScopeError, T], pos: Position): T =
        result match {
          case Right(r) => r
          case Left(e) => addScopeError(e, pos)
        }


      def analyzeJoinSelect(js: ast.JoinSelect): AtomicFrom[RNS, CT, CV] = {
        js match {
          case ast.JoinTable(tn) =>
            tn.aliasWithoutPrefix.foreach(ensureLegalAlias(_, NoPosition /* TODO: NEED POS INFO FROM AST */))
            analyzeForFrom(ScopedResourceName(scope, ResourceName(tn.nameWithoutPrefix)), canonicalName, NoPosition /* TODO: NEED POS INFO FROM AST */).
              reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
          case ast.JoinQuery(select, rawAlias) =>
            val alias = rawAlias.substring(1)
            ensureLegalAlias(alias, NoPosition /* TODO: NEED POS INFO FROM AST */)
            analyzeStatement(None, select, None).reAlias(Some(ResourceName(alias)))
          case ast.JoinFunc(tn, params) =>
            tn.aliasWithoutPrefix.foreach(ensureLegalAlias(_, NoPosition /* TODO: NEED POS INFO FROM AST */))
            analyzeUDF(ResourceName(tn.nameWithoutPrefix), params).
              reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
        }
      }

      def ensureLegalAlias(candidate: String, position: Position): Unit = {
        val rnCandidate = ResourceName(candidate)
        if(SoQLAnalyzer.SpecialNames(rnCandidate)) {
          reservedTableName(rnCandidate, position)
        }
      }

      def analyzeUDF(resource: ResourceName, params: Seq[ast.Expression]): AtomicFrom[RNS, CT, CV] = {
        val srn = ScopedResourceName(scope, resource)
        tableMap.find(srn) match {
          case TableDescription.TableFunction(udfScope, udfCanonicalName, parsed, _unparsed, paramSpecs) =>
            if(params.length != paramSpecs.size) {
              incorrectNumberOfParameters(resource, expected = params.length, got = paramSpecs.size, position = NoPosition /* TODO: NEED POS INFO FROM AST */)
            }
            // we're rewriting the UDF from
            //    @bleh(x, y, z)
            // to
            //    select * from (values (x,y,z)) join lateral (udfexpansion) on true
            // Possibly we'll want a rewrite pass that inlines constants and
            // maybe simple column references into the udf expansion, 

            val typecheckedParams =
              OrderedMap() ++ params.lazyZip(paramSpecs).map { case (expr, (name, typ)) =>
                name -> typecheck(expr, Map.empty, Some(typ))
              }

            NonEmptySeq.fromSeq(typecheckedParams.values.toVector) match {
              case Some(typecheckedExprs) =>
                val outOfLineParamsQuery = Values(NonEmptySeq(typecheckedExprs))
                val outOfLineParamsLabel = labelProvider.tableLabel()
                val innerUdfParams =
                  outOfLineParamsQuery.schema.keys.lazyZip(typecheckedParams).map { case (colLabel, (name, expr)) =>
                    name -> { (p: Position) => Column(outOfLineParamsLabel, colLabel, expr.typ)(new AtomicPositionInfo(p)) }
                  }.toMap

                val useQuery =
                  new Context(udfScope, Some(udfCanonicalName), Environment.empty, innerUdfParams)
                    .analyzeStatement(None, parsed, None)

                FromStatement(
                  Select(
                    Distinctiveness.Indistinct,
                    OrderedMap() ++ useQuery.statement.schema.iterator.map { case (label, NameEntry(name, typ)) =>
                      labelProvider.columnLabel() -> NamedExpr(Column(useQuery.label, label, typ)(AtomicPositionInfo.None), name)
                    },
                    Join(
                      JoinType.Inner,
                      true,
                      FromStatement(outOfLineParamsQuery, outOfLineParamsLabel, None, None),
                      useQuery,
                      typeInfo.literalBoolean(true, NoPosition)
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
                  Some(srn),
                  None
                )

              case None =>
                // There are no parameters, so we can just
                // expand the UDF without introducing a layer of
                // additional joining.
                new Context(udfScope, Some(udfCanonicalName), Environment.empty, Map.empty)
                  .analyzeStatement(Some(srn), parsed, None)
            }
          case _ =>
            // Non-UDF
            parametersForNonUdf(resource, position = NoPosition /* TODO: NEED POS INFO FROM AST */)
        }
      }


      def typecheck(
        expr: ast.Expression,
        namedExprs: Map[ColumnName, Expr[CT, CV]],
        expectedType: Option[CT],
      ): Expr[CT, CV] = {
        val tc = new Typechecker(scope, canonicalName, enclosingEnv, namedExprs, udfParams, userParameters, typeInfo, functionInfo)
        tc(expr, expectedType) match {
          case Right(e) => e
          case Left(err) => augmentTypecheckException(err)
        }
      }

      def verifyAggregatesAndWindowFunctions(stmt: Select[RNS, CT, CV]): Unit = {
        val isAggregated = stmt.isAggregated
        for(w <- stmt.where) verifyAggregatesAndWindowFunctions(w, false, false, Set.empty)
        for(gb <- stmt.groupBy) verifyAggregatesAndWindowFunctions(gb, false, false, Set.empty)
        val groupBys = stmt.groupBy.toSet
        for(h <- stmt.having) verifyAggregatesAndWindowFunctions(h, true, false, groupBys)
        for(ob <- stmt.orderBy) verifyAggregatesAndWindowFunctions(ob.expr, isAggregated, true, groupBys)
        for((_, s) <- stmt.selectList) verifyAggregatesAndWindowFunctions(s.expr, isAggregated, true, groupBys)
      }
      def verifyAggregatesAndWindowFunctions(e: Expr[CT, CV], allowAggregates: Boolean, allowWindow: Boolean, groupBys: Set[Expr[CT, CV]]): Unit = {
        e match {
          case _ : Literal[_, _] =>
            // literals are always ok
          case AggregateFunctionCall(f, args, _distinct, filter) if allowAggregates =>
            args.foreach(verifyAggregatesAndWindowFunctions(_, false, false, groupBys))
          case agg: AggregateFunctionCall[CT, CV] =>
            aggregateFunctionNotAllowed(agg.function.name, agg.position.functionNamePosition)
          case w@WindowedFunctionCall(f, args, filter, partitionBy, orderBy, _frame) if allowWindow =>
            // Fun fact: this order by does not have the same no-literals restriction as a select's order-by
            val subExprsAreAggregates = allowAggregates && w.isAggregated
            args.foreach(verifyAggregatesAndWindowFunctions(_, subExprsAreAggregates, false, groupBys))
            partitionBy.foreach(verifyAggregatesAndWindowFunctions(_, subExprsAreAggregates, false, groupBys))
            orderBy.foreach { ob => verifyAggregatesAndWindowFunctions(ob.expr, subExprsAreAggregates, false, groupBys) }
          case wfc: WindowedFunctionCall[CT, CV] =>
            windowFunctionNotAllowed(wfc.function.name, wfc.position.functionNamePosition)
          case e: Expr[CT, CV] if allowAggregates && groupBys.contains(e) =>
            // ok, we want an aggregate and this is an expression from the GROUP BY clause
          case FunctionCall(_f, args) =>
            // This is a valid aggregate if all our arguments are valid aggregates
            args.foreach(verifyAggregatesAndWindowFunctions(_, allowAggregates, allowWindow, groupBys))
          case c@Column(_table, _col, _typ) if allowAggregates =>
            // Column reference, but it's not from the group by clause.
            // Fail!
            ungroupedColumnReference(c.position.logicalPosition)
          case _ =>
            // ok
        }
      }

      def expectedBoolean(expr: ast.Expression, got: CT): Nothing =
        throw Bail(SoQLAnalyzerError.ExpectedBoolean(scope, canonicalName, typeInfo.typeNameFor(got), expr.position))
      def incorrectNumberOfParameters(forUdf: ResourceName, expected: Int, got: Int, position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.IncorrectNumberOfUdfParameters(scope, canonicalName, forUdf, expected, got, position))
      def distinctOnMustBePrefixOfOrderBy(position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.DistinctNotPrefixOfOrderBy(scope, canonicalName, position))
      def orderByMustBeSelected(position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.OrderByMustBeSelectedWhenDistinct(scope, canonicalName, position))
      def invalidGroupBy(typ: CT, position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.InvalidGroupBy(scope, canonicalName, typeInfo.typeNameFor(typ), position))
      def unorderedOrderBy(typ: CT, position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.UnorderedOrderBy(scope, canonicalName, typeInfo.typeNameFor(typ), position))
      def parametersForNonUdf(name: ResourceName, position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.ParametersForNonUDF(scope, canonicalName, name, position))
      def addScopeError(e: AddScopeError, position: Position): Nothing =
        e match {
          case AddScopeError.NameExists(n) =>
            throw Bail(SoQLAnalyzerError.AliasAlreadyExists(scope, canonicalName, n, position))
          case AddScopeError.MultipleImplicit =>
            // This shouldn't be able to occur from user input - the
            // grammar disallows it.
            throw new Exception("Multiple implicit tables??")
        }
      def noDataSource(position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.FromRequired(scope, canonicalName, position))
      def chainWithFrom(position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.FromForbidden(scope, canonicalName, position))
      def fromThisWithoutContext(position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.FromThisWithoutContext(scope, canonicalName, position))
      def tableOpTypeMismatch(left: OrderedMap[ColumnName, CT], right: OrderedMap[ColumnName, CT], position: Position): Nothing =
        throw Bail(SoQLAnalyzerError.TableOperationTypeMismatch(scope, canonicalName, left.valuesIterator.map(typeInfo.typeNameFor).toVector, right.valuesIterator.map(typeInfo.typeNameFor).toVector, position))
      def literalNotAllowedInGroupBy(pos: Position): Nothing =
        throw Bail(SoQLAnalyzerError.LiteralNotAllowedInGroupBy(scope, canonicalName, pos))
      def literalNotAllowedInOrderBy(pos: Position): Nothing =
        throw Bail(SoQLAnalyzerError.LiteralNotAllowedInOrderBy(scope, canonicalName, pos))
      def literalNotAllowedInDistinctOn(pos: Position): Nothing =
        throw Bail(SoQLAnalyzerError.LiteralNotAllowedInDistinctOn(scope, canonicalName, pos))
      def aggregateFunctionNotAllowed(name: FunctionName, pos: Position): Nothing =
        throw Bail(SoQLAnalyzerError.AggregateFunctionNotAllowed(scope, canonicalName, name, pos))
      def ungroupedColumnReference(pos: Position): Nothing =
        throw Bail(SoQLAnalyzerError.UngroupedColumnReference(scope, canonicalName, pos))
      def windowFunctionNotAllowed(name: FunctionName, pos: Position): Nothing =
        throw Bail(SoQLAnalyzerError.WindowFunctionNotAllowed(scope, canonicalName, name, pos))
      def reservedTableName(name: ResourceName, pos: Position): Nothing =
        throw Bail(SoQLAnalyzerError.ReservedTableName(scope, canonicalName, name, pos))
      def augmentAliasAnalysisException(aae: AliasAnalysisException): Nothing = {
        import com.socrata.soql.{exceptions => SE}

        val rewritten =
          aae match {
            case SE.RepeatedException(name, pos) =>
              SoQLAnalyzerError.AliasAnalysisError.RepeatedExclusion(scope, canonicalName, name, pos)
            case SE.CircularAliasDefinition(name, pos) =>
              SoQLAnalyzerError.AliasAnalysisError.CircularAliasDefinition(scope, canonicalName, name, pos)
            case SE.DuplicateAlias(name, pos) =>
              SoQLAnalyzerError.AliasAnalysisError.DuplicateAlias(scope, canonicalName, name, pos)
            case nsc@SE.NoSuchColumn(name, pos) =>
              val qual = nsc.asInstanceOf[SE.NoSuchColumn.RealNoSuchColumn].qualifier // ew
              SoQLAnalyzerError.TypecheckError.NoSuchColumn(scope, canonicalName, qual.map(_.substring(1)).map(ResourceName(_)), name, pos)
            case SE.NoSuchTable(_, _) =>
              throw new Exception("Alias analysis doesn't actually throw NoSuchTable")
          }

        throw Bail(rewritten)
      }
      def augmentTypecheckException(tce: SoQLAnalyzerError.TypecheckError[RNS]): Nothing =
        throw Bail(tce)
    }
    def illegalThisReference(scope: RNS, canonicalName: Option[CanonicalName], position: Position): Nothing =
      throw Bail(SoQLAnalyzerError.IllegalThisReference(scope, canonicalName, position))
    def parameterlessTableFunction(scope: RNS, canonicalName: Option[CanonicalName], name: ResourceName, position: Position): Nothing =
      throw Bail(SoQLAnalyzerError.ParameterlessTableFunction(scope, canonicalName, name, position))
  }
}
