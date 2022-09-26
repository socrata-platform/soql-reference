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
}

class SoQLAnalyzer[RNS, CT, CV](typeInfo: TypeInfo2[CT, CV], functionInfo: FunctionInfo[CT]) {
  type ScopedResourceName = (RNS, ResourceName)
  type TableMap = com.socrata.soql.analyzer2.TableMap[RNS, CT]
  type FoundTables = com.socrata.soql.analyzer2.FoundTables[RNS, CT]
  type ParsedTableDescription = com.socrata.soql.analyzer2.ParsedTableDescription[RNS, CT]
  type UserParameters = com.socrata.soql.analyzer2.UserParameters[CT, CV]

  type UdfParameters = Map[HoleName, Position => Expr[CT, CV]]

  private case class Bail(result: SoQLAnalyzerError[RNS]) extends Exception with NoStackTrace

  def apply(start: FoundTables, userParameters: UserParameters): Either[SoQLAnalyzerError[RNS], SoQLAnalysis[RNS, CT, CV]] = {
    try {
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

  private class State(tableMap: TableMap, userParameters: UserParameters) {
    val labelProvider = new LabelProvider

    def analyze(scope: RNS, query: FoundTables.Query): Statement[RNS, CT, CV] = {
      val from =
        query match {
          case FoundTables.Saved(rn) =>
            analyzeForFrom(scope, None, rn, NoPosition)
          case FoundTables.InContext(rn, q) =>
            val from = analyzeForFrom(scope, None, rn, NoPosition)
            new Context(scope, None, Environment.empty, Map.empty).
              analyzeStatement(q, Some(from))
          case FoundTables.InContextImpersonatingSaved(rn, q, impersonating) =>
            val from = analyzeForFrom(scope, None, rn, NoPosition)
            new Context(scope, Some(impersonating), Environment.empty, Map.empty).
              analyzeStatement(q, Some(from))
          case FoundTables.Standalone(q) =>
            new Context(scope, None, Environment.empty, Map.empty).
              analyzeStatement(q, None)
        }
      intoStatement(from)
    }

    def intoStatement(from: AtomicFrom[RNS, CT, CV]): Statement[RNS, CT, CV] = {
      val selectList =
        from match {
          case from: FromTableLike[RNS, CT] =>
            from.columns.map { case (label, NameEntry(name, typ)) =>
              labelProvider.columnLabel() -> NamedExpr(Column(from.label, label, typ)(NoPosition), name)
            }
          case from: FromSingleRow[RNS] =>
            OrderedMap.empty[AutoColumnLabel, NamedExpr[CT, CV]]
          case from: FromStatement[RNS, CT, CV] =>
            // Just short-circuit it and return the underlying Statement
            return from.statement
        }

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

    def fromTable(desc: ParsedTableDescription.Dataset[CT], alias: Option[(RNS, ResourceName)]): FromTable[RNS, CT] =
      FromTable(
        desc.name,
        alias,
        labelProvider.tableLabel(),
        columns = desc.schema
      )

    def analyzeForFrom(scope: RNS, canonicalName: Option[CanonicalName], rn: ResourceName, position: Position): AtomicFrom[RNS, CT, CV] = {
      tableMap.find(scope, rn) match {
        case ds: ParsedTableDescription.Dataset[CT] =>
          fromTable(ds, None)
        case ParsedTableDescription.Query(scope, canonicalName, basedOn, parsed, _unparsed, parameters) =>
          // so this is basedOn |> parsed
          // so we want to use "basedOn" as the implicit "from" for "parsed"
          val from = analyzeForFrom(scope, None, basedOn, NoPosition /* Yes, actually NoPosition here */)
          new Context(scope, Some(canonicalName), Environment.empty, Map.empty).
            analyzeStatement(parsed, Some(from))
        case ParsedTableDescription.TableFunction(_, _, _, _, _) =>
          parameterlessTableFunction(scope, canonicalName, rn, position)
      }
    }

    class Context(scope: RNS, canonicalName: Option[CanonicalName], enclosingEnv: Environment[CT], udfParams: UdfParameters) {
      private def withEnv(env: Environment[CT]) = new Context(scope, canonicalName, env, udfParams)

      def analyzeStatement(q: BinaryTree[ast.Select], from0: Option[AtomicFrom[RNS, CT, CV]]): FromStatement[RNS, CT, CV] = {
        q match {
          case Leaf(select) =>
            analyzeSelection(select, from0)
          case PipeQuery(left, right) =>
            val newInput = analyzeStatement(left, from0)
            analyzeStatement(right, Some(newInput))
          case other: TrueOp[ast.Select] =>
            val lhs = intoStatement(analyzeStatement(other.left, from0))
            val rhs = intoStatement(analyzeStatement(other.right, None))

            if(lhs.schema.values.map(_.typ) != rhs.schema.values.map(_.typ)) {
              tableOpTypeMismatch(
                OrderedMap() ++ lhs.schema.valuesIterator.map { case NameEntry(name, typ) => name -> typ },
                OrderedMap() ++ rhs.schema.valuesIterator.map { case NameEntry(name, typ) => name -> typ },
                NoPosition /* TODO: NEED POS INFO FROM AST */
              )
            }

            FromStatement(CombinedTables(opify(other), lhs, rhs), labelProvider.tableLabel(), None)
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

      def analyzeSelection(select: ast.Select, from0: Option[AtomicFrom[RNS, CT, CV]]): FromStatement[RNS, CT, CV] = {
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
            invalidGroupBy(checked.typ, checked.position)
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
            unorderedOrderBy(checked.typ, checked.position)
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
                  unorderedOrderBy(expr.typ, expr.position)
                }
              }
            }
          case _ =>
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
            }
          }.toSet
        )

        verifyAggregatesAndWindowFunctions(stmt)

        FromStatement(stmt, labelProvider.tableLabel(), None)
      }

      def collectNamesForAnalysis(from: From[RNS, CT, CV]): AliasAnalysis.AnalysisContext = {
        def contextFrom(af: AtomicFrom[RNS, CT, CV]): UntypedDatasetContext =
          af match {
            case _: FromSingleRow[RNS] => new UntypedDatasetContext {
              val columns = OrderedSet.empty
            }
            case t: FromTableLike[RNS, CT] => new UntypedDatasetContext {
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
            case Some((_scope, alias)) =>
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
            val rn = ResourceName(tn.nameWithoutPrefix)
            if(rn == SoQLAnalyzer.This) {
              // chained query: {something} |> select ... from @this [as alias]
              prev.reAlias(Some((scope, ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix)))))
            } else {
              // chained query: {something} |> select ... from somethingThatIsNotThis
              // this is an error
              chainWithFrom(NoPosition /* TODO: NEED POS INFO FROM AST */)
            }
          case (None, Some(tn)) =>
            val rn = ResourceName(tn.nameWithoutPrefix)
            val alias = ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))
            if(rn == SoQLAnalyzer.This) {
              // standalone query: select ... from @this.  But it's standalone, so this is an error
              fromThisWithoutContext(NoPosition /* TODO: NEED POS INFO FROM AST */)
            } else if(rn == SoQLAnalyzer.SingleRow) {
              FromSingleRow(labelProvider.tableLabel(), Some((scope, alias)))
            } else {
              // standalone query: select ... from sometable ...
              // n.b., sometable may actually be a query
              analyzeForFrom(scope, canonicalName, rn, NoPosition /* TODO: NEED POS INFO FROM AST */).
                reAlias(Some((scope, alias)))
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
            analyzeForFrom(scope, canonicalName, ResourceName(tn.nameWithoutPrefix), NoPosition /* TODO: NEED POS INFO FROM AST */).
              reAlias(Some((scope, ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix)))))
          case ast.JoinQuery(select, alias) =>
            analyzeStatement(select, None).reAlias(Some((scope, ResourceName(alias.substring(1)))))
          case ast.JoinFunc(tn, params) =>
            analyzeUDF(tn, params)
        }
      }

      def analyzeUDF(tableName: TableName, params: Seq[ast.Expression]): AtomicFrom[RNS, CT, CV] = {
        val resource = ResourceName(tableName.nameWithoutPrefix)
        tableMap.find(scope, resource) match {
          case ParsedTableDescription.TableFunction(udfScope, udfCanonicalName, parsed, _unparsed, paramSpecs) =>
            if(params.length != paramSpecs.size) {
              incorrectNumberOfParameters(resource, expected = params.length, got = paramSpecs.size, position = NoPosition /* TODO: NEED POS INFO FROM AST */)
            }
            // we're rewriting the UDF from
            //    @bleh(x, y, z)
            // to
            //    select * from (values (x,y,z)) join lateral (udfexpansion) on true
            // though this only happens for params which expand to function calls.
            // Literal and column refs just get inlined into the udfexpansion.

            val typecheckedParams =
              OrderedMap() ++ params.lazyZip(paramSpecs).map { case (expr, (name, typ)) =>
                name -> typecheck(expr, Map.empty, Some(typ))
              }

            val (outOfLineParams, inlineParams) = typecheckedParams.partition { case (_name, e) =>
                e match {
                  case _ : FuncallLike[CT, CV] => true
                  case _ : Column[CT] | _ : Literal[CT, CV] => false
                  case _ : SelectListReference[CT] => throw new Exception("Impossible: select list reference out of typechecking")
                }
              }

            NonEmptySeq.fromSeq(outOfLineParams.values.toVector) match {
              case Some(outOfLineValues) =>
                val outOfLineParamsQuery = Values(NonEmptySeq(outOfLineValues))
                val outOfLineParamsLabel = labelProvider.tableLabel()
                val innerUdfParams =
                  outOfLineParamsQuery.schema.keys.lazyZip(outOfLineParams).map { case (colLabel, (name, expr)) =>
                    name -> { (p: Position) => Column(outOfLineParamsLabel, colLabel, expr.typ)(p) }
                  }.toMap ++ inlineParams.withValuesMapped { c =>
                    // reposition the inline parameter so that if an
                    // error is reported while typechecking (or,
                    // eventually, running) the UDF body, it gets
                    // reported at the parameter-use-site rather than
                    // the parameter-call-site.
                    c.reposition _
                  }

                val useQuery =
                  new Context(udfScope, Some(udfCanonicalName), Environment.empty, innerUdfParams)
                    .analyzeStatement(parsed, None)

                FromStatement(
                  Select(
                    Distinctiveness.Indistinct,
                    OrderedMap() ++ useQuery.statement.schema.iterator.map { case (label, NameEntry(name, typ)) =>
                      labelProvider.columnLabel() -> NamedExpr(Column(useQuery.label, label, typ)(NoPosition), name)
                    },
                    Join(
                      JoinType.Inner,
                      true,
                      FromStatement(outOfLineParamsQuery, outOfLineParamsLabel, None),
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
                  Some((scope, ResourceName(tableName.aliasWithoutPrefix.getOrElse(tableName.nameWithoutPrefix))))
                )

              case None =>
                // There are no out-of-line values, so we can just
                // expand the UDF without introducing a layer of
                // additional joining.
                val innerUdfParams = inlineParams.withValuesMapped { c => c.reposition _ }.toMap
                new Context(udfScope, Some(udfCanonicalName), Environment.empty, innerUdfParams)
                  .analyzeStatement(parsed, None)
                  .reAlias(Some((scope, ResourceName(tableName.aliasWithoutPrefix.getOrElse(tableName.nameWithoutPrefix)))))
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
            aggregateFunctionNotAllowed(agg.function.name, agg.functionNamePosition)
          case w@WindowedFunctionCall(f, args, filter, partitionBy, orderBy, _frame) if allowWindow =>
            // Fun fact: this order by does not have the same no-literals restriction as a select's order-by
            val subExprsAreAggregates = allowAggregates && w.isAggregated
            args.foreach(verifyAggregatesAndWindowFunctions(_, subExprsAreAggregates, false, groupBys))
            partitionBy.foreach(verifyAggregatesAndWindowFunctions(_, subExprsAreAggregates, false, groupBys))
            orderBy.foreach { ob => verifyAggregatesAndWindowFunctions(ob.expr, subExprsAreAggregates, false, groupBys) }
          case wfc: WindowedFunctionCall[CT, CV] =>
            windowFunctionNotAllowed(wfc.function.name, wfc.functionNamePosition)
          case e: Expr[CT, CV] if allowAggregates && groupBys.contains(e) =>
            // ok, we want an aggregate and this is an expression from the GROUP BY clause
          case FunctionCall(_f, args) =>
            // This is a valid aggregate if all our arguments are valid aggregates
            args.foreach(verifyAggregatesAndWindowFunctions(_, allowAggregates, allowWindow, groupBys))
          case c@Column(_table, _col, _typ) if allowAggregates =>
            // Column reference, but it's not from the group by clause.
            // Fail!
            ungroupedColumnReference(c.position)
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
    def parameterlessTableFunction(scope: RNS, canonicalName: Option[CanonicalName], name: ResourceName, position: Position): Nothing =
      throw Bail(SoQLAnalyzerError.ParameterlessTableFunction(scope, canonicalName, name, position))
  }
}
