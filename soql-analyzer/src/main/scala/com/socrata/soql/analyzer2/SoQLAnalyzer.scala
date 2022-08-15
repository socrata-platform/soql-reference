// TODO:
//  * Errors
//     - everything needs position info
//     - that position info needs to include query source (scope, Option[ResourceName])
//  * Make it possible to find label-isomorphisms between Statements
//    and transform an isomorphic statement into an equal statement

package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.parsing.input.{Position, NoPosition}
import scala.collection.compat._

import com.socrata.soql.{BinaryTree, Leaf, TrueOp, PipeQuery, UnionQuery, UnionAllQuery, IntersectQuery, IntersectAllQuery, MinusQuery, MinusAllQuery}
import com.socrata.soql.ast
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName, HoleName, UntypedDatasetContext}
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

  def apply(start: FoundTables, userParameters: UserParameters): SoQLAnalysis[RNS, CT, CV] = {
    val state = new State(start.tableMap, userParameters)

    new SoQLAnalysis[RNS, CT, CV](
      state.labelProvider,
      state.analyze(start.initialScope, start.initialQuery)
    )
  }

  private class State(tableMap: TableMap, userParameters: UserParameters) {
    val labelProvider = new LabelProvider(t => s"t$t", c => s"c$c")

    def analyze(scope: RNS, query: FoundTables.Query): Statement[RNS, CT, CV] = {
      val from =
        query match {
          case FoundTables.Saved(rn) =>
            analyzeForFrom(scope, rn, Environment.empty)
          case FoundTables.InContext(rn, q) =>
            val from = analyzeForFrom(scope, rn, Environment.empty)
            new Context(scope, None, Environment.empty, Map.empty).
              analyzeStatement(q, Some(from))
          case FoundTables.InContextImpersonatingSaved(rn, q, impersonating) =>
            val from = analyzeForFrom(scope, rn, Environment.empty)
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

    def analyzeForFrom(scope: RNS, rn: ResourceName, env: Environment[CT]): AtomicFrom[RNS, CT, CV] = {
      tableMap.find(scope, rn) match {
        case ds: ParsedTableDescription.Dataset[CT] =>
          fromTable(ds, None)
        case ParsedTableDescription.Query(scope, canonicalName, basedOn, parsed, parameters) =>
          // so this is basedOn |> parsed
          // so we want to use "basedOn" as the implicit "from" for "parsed"
          val from = analyzeForFrom(scope, basedOn, env)
          new Context(scope, Some(canonicalName), env, Map.empty).
            analyzeStatement(parsed, Some(from))
        case ParsedTableDescription.TableFunction(_, _, _, _) =>
          tableFunctionInIncorrectPosition()
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
              tableOpTypeMismatch()
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
        val localEnv = envify(completeFrom.extendEnvironment(enclosingEnv))

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
        class EvaluationState(val namedExprs: OrderedMap[ColumnName, Expr[CT, CV]] = OrderedMap.empty) {
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
                  constantInDistinctOn(expr.position)
                }
                finalState.typecheck(expr)
              }
            )
        }

        val checkedWhere = where.map(finalState.typecheck(_, Some(typeInfo.boolType)))
        val checkedGroupBys = groupBys.map { expr =>
          if(expr.isInstanceOf[ast.Literal]) {
            constantInGroupBy(expr.position)
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
            constantInOrderBy(expr.position)
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
                distinctOnMustBePrefixOfOrderBy()
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

        for(pos <- checkedGroupBys.collect { case l: Literal[_, _] => l.position }.headOption) {
          literalNotAllowedInGroupBy(pos)
        }
        for(pos <- checkedOrderBys.collect { case OrderBy(l: Literal[_, _], _, _) => l.position }.headOption) {
          literalNotAllowedInOrderBy(pos)
        }

        val stmt = Select(
          checkedDistinct,
          finalState.namedExprs.map { case (cn, expr) => labelProvider.columnLabel() -> NamedExpr(expr, cn) },
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

        @tailrec
        def loop(from: From[RNS, CT, CV], acc: Map[AliasAnalysis.Qualifier, UntypedDatasetContext]): Map[AliasAnalysis.Qualifier, UntypedDatasetContext] = {
          from match {
            case j: Join[RNS, CT, CV] =>
              loop(j.right, augmentAcc(acc, j.left))
            case a: AtomicFrom[RNS, CT, CV] =>
              augmentAcc(acc, a)
          }
        }
        loop(from, Map.empty)
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

        var mostRecentFrom: AtomicFrom[RNS, CT, CV] = (input, from) match {
          case (None, None) =>
            // No context and no from; this is an error
            noDataSource()
          case (Some(prev), Some(tn)) =>
            val rn = ResourceName(tn.nameWithoutPrefix)
            if(rn == SoQLAnalyzer.This) {
              // chained query: {something} |> select ... from @this [as alias]
              prev.reAlias(Some((scope, ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix)))))
            } else {
              // chained query: {something} |> select ... from somethingThatIsNotThis
              // this is an error
              chainWithFrom()
            }
          case (None, Some(tn)) =>
            val rn = ResourceName(tn.nameWithoutPrefix)
            val alias = ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))
            if(rn == SoQLAnalyzer.This) {
              // standalone query: select ... from @this.  But it's standalone, so this is an error
              thisWithoutContext()
            } else if(rn == SoQLAnalyzer.SingleRow) {
              FromSingleRow(labelProvider.tableLabel(), Some((scope, alias)))
            } else {
              // standalone query: select ... from sometable ...
              // n.b., sometable may actually be a query
              analyzeForFrom(scope, rn, enclosingEnv).
                reAlias(Some((scope, alias)))
            }
          case (Some(input), None) =>
            // chained query: {something} |> {the thing we're analyzing}
            input.reAlias(None)
        }

        var pendingJoins = Vector[From[RNS, CT, CV] => From[RNS, CT, CV]]()
        def fromSoFar(): From[RNS, CT, CV] = {
          pendingJoins.foldRight[From[RNS, CT, CV]](mostRecentFrom) { (part, acc) => part(acc) }
        }

        for(join <- joins) {
          val joinType = join.typ match {
            case ast.InnerJoinType => JoinType.Inner
            case ast.LeftOuterJoinType => JoinType.LeftOuter
            case ast.RightOuterJoinType => JoinType.RightOuter
            case ast.FullOuterJoinType => JoinType.FullOuter
          }

          val augmentedFrom = envify(fromSoFar().extendEnvironment(enclosingEnv))
          val effectiveLateral = join.lateral || join.from.isInstanceOf[ast.JoinFunc]
          val checkedFrom =
            if(effectiveLateral) {
              withEnv(augmentedFrom).analyzeJoinSelect(join.from)
            } else {
              analyzeJoinSelect(join.from)
            }

          val checkedOn = withEnv(envify(checkedFrom.addToEnvironment(augmentedFrom))).
            typecheck(join.on, Map.empty, Some(typeInfo.boolType))

          if(!typeInfo.isBoolean(checkedOn.typ)) {
            expectedBoolean(join.on, checkedOn.typ)
          }

          val left = mostRecentFrom
          pendingJoins +:= { (right: From[RNS, CT, CV]) => Join(joinType, effectiveLateral, left, right, checkedOn) }
          mostRecentFrom = checkedFrom
        }

        fromSoFar()
      }

      def envify[T](result: Either[AddScopeError, T]): T =
        result match {
          case Right(r) => r
          case Left(e) => addScopeError(e)
        }


      def analyzeJoinSelect(js: ast.JoinSelect): AtomicFrom[RNS, CT, CV] = {
        js match {
          case ast.JoinTable(tn) =>
            analyzeForFrom(scope, ResourceName(tn.nameWithoutPrefix), enclosingEnv).
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
          case ParsedTableDescription.TableFunction(udfScope, udfCanonicalName, parsed, paramSpecs) =>
            if(params.length != paramSpecs.size) {
              incorrectNumberOfParameters(resource, expected = params.length, got = paramSpecs.size)
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
                  new Context(udfScope, Some(udfCanonicalName), enclosingEnv, innerUdfParams)
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
                new Context(udfScope, Some(udfCanonicalName), enclosingEnv, innerUdfParams)
                  .analyzeStatement(parsed, None)
                  .reAlias(Some((scope, ResourceName(tableName.aliasWithoutPrefix.getOrElse(tableName.nameWithoutPrefix)))))
            }
          case _ =>
            // Non-UDF
            parametersForNonUdf(resource)
        }
      }


      def typecheck(
        expr: ast.Expression,
        namedExprs: Map[ColumnName, Expr[CT, CV]],
        expectedType: Option[CT],
      ): Expr[CT, CV] = {
        val tc = new Typechecker(enclosingEnv, namedExprs, udfParams, userParameters, canonicalName, typeInfo, functionInfo)
        try {
          tc(expr, expectedType)
        } catch {
          case e: tc.TypecheckError =>
            augmentTypecheckException(e)
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
            aggregateFunctionNotAllowed(agg.position)
          case w@WindowedFunctionCall(f, args, filter, partitionBy, orderBy, _frame) if allowWindow =>
            // Fun fact: this order by does not have the same no-literals restriction as a select's order-by
            val subExprsAreAggregates = allowAggregates && w.isAggregated
            args.foreach(verifyAggregatesAndWindowFunctions(_, subExprsAreAggregates, false, groupBys))
            partitionBy.foreach(verifyAggregatesAndWindowFunctions(_, subExprsAreAggregates, false, groupBys))
            orderBy.foreach { ob => verifyAggregatesAndWindowFunctions(ob.expr, subExprsAreAggregates, false, groupBys) }
          case wfc: WindowedFunctionCall[CT, CV] =>
            windowFunctionNotAllowed(wfc.functionNamePosition)
          case e: Expr[CT, CV] if allowAggregates && groupBys.contains(e) =>
            // ok, we want an aggregate and this is an expression from the GROUP BY clause
          case FunctionCall(_f, args) =>
            // This is a valid aggregate if all our arguments are valid aggregates
            args.foreach(verifyAggregatesAndWindowFunctions(_, allowAggregates, allowWindow, groupBys))
          case c@Column(_table, _col, _typ) if allowAggregates =>
            // Column reference, but it's not from the group by clause.
            // Fail!
            aggregateRequired(c.position)
          case _ =>
            // ok
        }
      }
    }

    def expectedBoolean(expr: ast.Expression, got: CT): Nothing = ???
    def incorrectNumberOfParameters(forUdf: ResourceName, expected: Int, got: Int): Nothing = ???
    def distinctOnMustBePrefixOfOrderBy(): Nothing = ???
    def tableFunctionInIncorrectPosition(): Nothing = ???
    def invalidGroupBy(typ: CT, position: Position): Nothing = ???
    def unorderedOrderBy(typ: CT, position: Position): Nothing = ???
    def parametersForNonUdf(name: ResourceName): Nothing = ???
    def udfParameterTypeMismatch(position: Position, expected: CT, got: CT): Nothing = ???
    def addScopeError(e: AddScopeError): Nothing = ???
    def noDataSource(): Nothing = ???
    def chainWithFrom(): Nothing = ???
    def thisWithoutContext(): Nothing = ???
    def tableOpTypeMismatch(): Nothing = ???
    def literalNotAllowedInGroupBy(pos: Position): Nothing = ???
    def literalNotAllowedInOrderBy(pos: Position): Nothing = ???
    def aggregateFunctionNotAllowed(pos: Position): Nothing = ???
    def aggregateRequired(pos: Position): Nothing = ???
    def windowFunctionNotAllowed(pos: Position): Nothing = ???
    def augmentAliasAnalysisException(aae: AliasAnalysisException): Nothing = throw aae
    def augmentTypecheckException[CT, CV](tce: Typechecker[CT, CV]#TypecheckError): Nothing = throw tce
    def constantInGroupBy(pos: Position): Nothing = ???
    def constantInOrderBy(pos: Position): Nothing = ???
    def constantInDistinctOn(pos: Position): Nothing = ???
 }
}
