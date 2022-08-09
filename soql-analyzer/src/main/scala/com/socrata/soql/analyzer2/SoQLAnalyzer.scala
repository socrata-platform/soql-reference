package com.socrata.soql.analyzer2

import scala.util.parsing.input.{Position, NoPosition}
import scala.collection.compat._

import com.socrata.soql.{BinaryTree, Leaf, TrueOp, PipeQuery, UnionQuery, UnionAllQuery, IntersectQuery, IntersectAllQuery, MinusQuery, MinusAllQuery}
import com.socrata.soql.ast
import com.socrata.soql.collection.{OrderedMap, OrderedSet}
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName, HoleName, UntypedDatasetContext}
import com.socrata.soql.typechecker.{TypeInfo, FunctionInfo, HasType}
import com.socrata.soql.aliases.AliasAnalysis

abstract class SoQLAnalysis[CT, CV] {
  val statement: Statement[CT, CV]
}

case class TypedNull[+CT](typ: CT)

class SoQLAnalyzer[RNS, CT, CV](typeInfo: TypeInfo[CT, CV], functionInfo: FunctionInfo[CT]) {
  type ScopedResourceName = (RNS, ResourceName)
  type TableMap = com.socrata.soql.analyzer2.TableMap[RNS, CT]
  type FoundTables = com.socrata.soql.analyzer2.FoundTables[RNS, CT]
  type ParsedTableDescription = com.socrata.soql.analyzer2.ParsedTableDescription[RNS, CT]

  type UserParameters = Map[String, Map[HoleName, Either[TypedNull[CT], CV]]]
  type UdfParameters = Map[HoleName, Position => Column[CT]]

  def apply(start: FoundTables, userParameters: UserParameters): SoQLAnalysis[CT, CV] = {
    val state = new State(start.tableMap, userParameters)

    new SoQLAnalysis[CT, CV] {
      val statement = state.analyze(start.initialScope, start.initialQuery)
    }
  }

  private class State(tableMap: TableMap, userParameters: UserParameters) {
    val labelProvider = new LabelProvider(t => s"t$t", c => s"c$c")

    def analyze(scope: RNS, query: FoundTables.Query): Statement[CT, CV] = {
      val from =
        query match {
          case FoundTables.Saved(rn) =>
            analyzeForContext(tableMap.find(scope, rn), Environment.empty)
          case FoundTables.InContext(rn, q) =>
            val context = analyzeForContext(tableMap.find(scope, rn), Environment.empty)
            analyzeInContext(scope, q, Some(context), Environment.empty, Map.empty)
          case FoundTables.Standalone(q) =>
            analyzeInContext(scope, q, None, Environment.empty, Map.empty)
        }
      intoStatement(from)
    }

    def intoStatement(from: AtomicFrom[CT, CV]): Statement[CT, CV] = {
      val selectList =
        from match {
          case from: FromTableLike[CT] =>
            from.columns.map { case (label, NameEntry(name, typ)) =>
              labelProvider.columnLabel() -> NamedExpr(Column(from.label, label, typ)(NoPosition), name)
            }
          case from: FromSingleRow =>
            OrderedMap.empty[AutoColumnLabel, NamedExpr[CT, CV]]
          case from: FromStatement[CT, CV] =>
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

    def fromTable(desc: ParsedTableDescription.Dataset[CT], alias: Option[ResourceName]): FromTable[CT] =
      FromTable(
        desc.name,
        alias,
        labelProvider.tableLabel(),
        columns = desc.schema
      )

    def analyzeForContext(desc: ParsedTableDescription, env: Environment[CT]): AtomicFrom[CT, CV] = {
      desc match {
        case ds: ParsedTableDescription.Dataset[CT] =>
          fromTable(ds, None)
        case ParsedTableDescription.Query(scope, _canonicalName, basedOn, parsed, parameters) =>
          // so this is basedOn |> parsed
          // so we want to analyze "basedOn" in the env we're given
          // and then parsed in that env extended with the result of basedOn
          val from = analyzeForContext(tableMap.find(scope, basedOn), env)
          analyzeInContext(scope, parsed, Some(from), env, Map.empty)
        case ParsedTableDescription.TableFunction(_, _, _, _) =>
          tableFunctionInIncorrectPosition()
      }
    }

    def analyzeInContext(scope: RNS, q: BinaryTree[ast.Select], from0: Option[AtomicFrom[CT, CV]], env: Environment[CT], udfParams: UdfParameters): AtomicFrom[CT, CV] = {
      q match {
        case Leaf(select) =>
          analyzeSelection(scope, select, from0, env, udfParams)
        case PipeQuery(left, right) =>
          val newInput = analyzeInContext(scope, left, from0, env, udfParams)
          analyzeInContext(scope, right, Some(newInput), env, udfParams)
        case other: TrueOp[ast.Select] =>
          val lhs = intoStatement(analyzeInContext(scope, other.left, from0, env, udfParams))
          val rhs = intoStatement(analyzeInContext(scope, other.right, None, env, udfParams))

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

    def analyzeSelection(scope: RNS, select: ast.Select, from0: Option[AtomicFrom[CT, CV]], env: Environment[CT], udfParams: UdfParameters): FromStatement[CT, CV] = {
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

      // ok, first things first: establish the schema we're operating
      // in, which means rolling up "from" and "joins" into a From

      val completeFrom = queryInputSchema(scope, from0, from, joins, env, udfParams)

      val localEnv = envify(completeFrom.extendEnvironment(env))

      // Now that we know what we're selecting from, we'll give names to the selection...
      val aliasAnalysis = AliasAnalysis(selection, from)(collectNamesForAnalysis(completeFrom))

      // With the aliases assigned we'll typecheck the select-list.
      //
      // Complications here:
      //   1. bleh as blah introduces a new name into scope.  We'll have to augment the
      //      environment with a way to find the (typechecked) expansions of those names
      //   2. During typechecking we'll need (?) to keep track of the window/aggregate/normal
      //      state.  The old analyzer does that in a secondary pass; perhaps we should
      //      too.

      class EvaluationState private (val env: Environment[CT], val namedExprs: OrderedMap[ColumnName, Expr[CT, CV]]) {
        def this(env: Environment[CT]) = this(env, OrderedMap())
        def update(name: ColumnName, expr: Expr[CT, CV]): EvaluationState = {
          new EvaluationState(env, namedExprs + (name -> expr))
        }

        def typecheck(expr: ast.Expression) = State.this.typecheck(expr, env, namedExprs, udfParams, None)
      }

      val finalState = aliasAnalysis.evaluationOrder.foldLeft(new EvaluationState(localEnv)) { (state, colName) =>
        val expression = aliasAnalysis.expressions(colName)
        val typed = state.typecheck(expression)
        state.update(colName, typed)
      }

      val checkedDistinct = distinct match {
        case ast.Indistinct => Distinctiveness.Indistinct
        case ast.FullyDistinct => Distinctiveness.FullyDistinct
        case ast.DistinctOn(exprs) =>
          Distinctiveness.On(exprs.map(finalState.typecheck))
      }

      val checkedWhere = where.map(finalState.typecheck)
      val checkedGroupBys = groupBys.map(finalState.typecheck)
      for(cgb <- checkedGroupBys) {
        if(!typeInfo.isGroupable(cgb.typ)) {
          invalidGroupBy(cgb.typ, cgb.position)
        }
      }
      val checkedHaving = having.map(finalState.typecheck)
      val checkedOrderBys = orderBys.map { case ast.OrderBy(expr, ascending, nullLast) =>
        val checked = finalState.typecheck(expr)
        if(!typeInfo.isOrdered(checked.typ)) {
          unorderedOrderBy(checked.typ, checked.position)
        }
        OrderBy(checked, ascending, nullLast)
      }

      checkedDistinct match {
        case Distinctiveness.On(exprs) =>
          if(!checkedOrderBys.map(_.expr).startsWith(exprs)) {
            distinctOnMustBePrefixOfOrderBy()
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

    def collectNamesForAnalysis(from: From[CT, CV]): AliasAnalysis.AnalysisContext = {
      def contextFrom(af: AtomicFrom[CT, CV]): UntypedDatasetContext =
        af match {
          case _: FromSingleRow => new UntypedDatasetContext {
            val columns = OrderedSet.empty
          }
          case t: FromTableLike[CT] => new UntypedDatasetContext {
            val columns = OrderedSet() ++ t.columns.valuesIterator.map(_.name)
          }
          case s: FromStatement[CT, CV] => new UntypedDatasetContext {
            val columns = OrderedSet() ++ s.statement.schema.valuesIterator.map(_.name)
          }
        }

      def augmentAcc(acc: Map[AliasAnalysis.Qualifier, UntypedDatasetContext], from: AtomicFrom[CT, CV]) = {
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

      def loop(from: From[CT, CV], acc: Map[AliasAnalysis.Qualifier, UntypedDatasetContext]): Map[AliasAnalysis.Qualifier, UntypedDatasetContext] = {
        from match {
          case j: Join[CT, CV] =>
            loop(j.right, augmentAcc(acc, j.left))
          case a: AtomicFrom[CT, CV] =>
            augmentAcc(acc, a)
        }
      }
      loop(from, Map.empty)
    }

    def queryInputSchema(scope: RNS, input: Option[AtomicFrom[CT, CV]], from: Option[TableName], joins: Seq[ast.Join], enclosingEnv: Environment[CT], udfParams: UdfParameters): From[CT, CV] = {
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

      var mostRecentFrom: AtomicFrom[CT, CV] = (input, from) match {
        case (None, None) =>
          // No context and no from; this is an error
          noDataSource()
        case (Some(prev), Some(tn@TableName(TableName.This, _))) =>
          // chained query: {something} |> select ... from @this [as alias]
          prev.reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
        case (Some(_), Some(_)) =>
          // chained query: {something} |> select ... from somethingThatIsNotThis
          // this is an error
          chainWithFrom()
        case (None, Some(tn@TableName(TableName.This, _))) =>
          // standalone query: select ... from @this.  But it's standalone, so this is an error
          thisWithoutContext()
        case (None, Some(tn@TableName(TableName.SingleRow, _))) =>
          FromSingleRow(labelProvider.tableLabel(), Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
        case (None, Some(tn)) =>
          // standalone query: select ... from sometable ...
          // n.b., sometable may actually be a query
          analyzeForContext(
            tableMap.find(scope, ResourceName(tn.nameWithoutPrefix)),
            enclosingEnv
          ).reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
        case (Some(input), None) =>
          // chained query: {something} |> {the thing we're analyzing}
          input.reAlias(None)
      }

      var pendingJoins = Vector[From[CT, CV] => From[CT, CV]]()
      def fromSoFar(): From[CT, CV] = {
        pendingJoins.foldRight[From[CT, CV]](mostRecentFrom) { (part, acc) => part(acc) }
      }

      for(join <- joins) {
        val joinType = join.typ match {
          case ast.InnerJoinType => JoinType.Inner
          case ast.LeftOuterJoinType => JoinType.LeftOuter
          case ast.RightOuterJoinType => JoinType.RightOuter
          case ast.FullOuterJoinType => JoinType.FullOuter
        }

        val augmentedFrom = envify(fromSoFar().extendEnvironment(enclosingEnv))
        val checkedFrom =
          if(join.lateral) {
            analyzeJoinSelect(scope, join.from, augmentedFrom, udfParams)
          } else {
            analyzeJoinSelect(scope, join.from, enclosingEnv, udfParams)
          }

        val checkedOn = typecheck(join.on, envify(checkedFrom.addToEnvironment(augmentedFrom)), Map.empty, udfParams, Some(typeInfo.boolType))

        if(!typeInfo.isBoolean(checkedOn.typ)) {
          expectedBoolean(join.on, checkedOn.typ)
        }

        val left = mostRecentFrom
        pendingJoins +:= { (right: From[CT, CV]) => Join(joinType, join.lateral, left, right, checkedOn) }
        mostRecentFrom = checkedFrom
      }

      fromSoFar()
    }

    def envify[T](result: Either[AddScopeError, T]): T =
      result match {
        case Right(r) => r
        case Left(e) => addScopeError(e)
      }

    def analyzeJoinSelect(scope: RNS, js: ast.JoinSelect, env: Environment[CT], udfParams: UdfParameters): AtomicFrom[CT, CV] = {
      js match {
        case ast.JoinTable(tn) =>
          analyzeForContext(tableMap.find(scope, ResourceName(tn.nameWithoutPrefix)), env).reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
        case ast.JoinQuery(select, alias) =>
          analyzeInContext(scope, select, None, env, udfParams).reAlias(Some(ResourceName(alias)))
        case ast.JoinFunc(tn, params) =>
          analyzeUDF(scope, tn, params, env, udfParams)
      }
    }

    def analyzeUDF(scope: RNS, tableName: TableName, params: Seq[ast.Expression], env: Environment[CT], outerUdfParams: UdfParameters): AtomicFrom[CT, CV] = {
      val resource = ResourceName(tableName.nameWithoutPrefix)
      tableMap.find(scope, resource) match {
        case ParsedTableDescription.TableFunction(udfScope, _canonicalName, parsed, paramSpecs) =>
          if(params.length != paramSpecs.size) {
            incorrectNumberOfParameters(resource, expected = params.length, got = paramSpecs.size)
          }
          // we're rewriting the UDF from
          //    @bleh(x, y, z)
          // to
          //    with $label (values(x,y,z)) udfexpansion
          val definitionQuery =
            Values(
              params.lazyZip(paramSpecs).map { case (expr, (_name, typ)) =>
                typecheck(expr, env, Map.empty, outerUdfParams, Some(typ))
              }
            )
          val definitionLabel = labelProvider.tableLabel()
          val definitionUseLabel = labelProvider.tableLabel()
          val innerUdfParams = definitionQuery.schema.keys.lazyZip(paramSpecs).map { case (colLabel, (name, typ)) =>
            name -> { (p: Position) => Column(definitionUseLabel, colLabel, typ)(p) }
          }.toMap
          val useQuery = intoStatement(
            analyzeInContext(
              udfScope,
              // For historical reasons, UDF queries were written as
              // `select blahblah FROM @single_row ...` but we're
              // providing the definition differently now and it's not
              // needed anymore, so remove it if it's present.
              removeSingleRowFrom(parsed),
              // This is a little ugly; what's happening here is that
              // the databasetablename of the CTE is is _also_ a label
              // (and in fact there is no way to give it a different
              // alias in the sql).
              Some(FromVirtualTable(
                     definitionLabel,
                     None,
                     definitionUseLabel,
                     definitionQuery.typeVariedSchema
                   )),
              env,
              innerUdfParams
            )
          )

          FromStatement(
            CTE(definitionLabel, definitionQuery, MaterializedHint.Materialized, useQuery),
            labelProvider.tableLabel(),
            Some(ResourceName(tableName.aliasWithoutPrefix.getOrElse(tableName.nameWithoutPrefix)))
          )
        case _ =>
          // Non-UDF
          parametersForNonUdf(resource)
      }
    }

    def removeSingleRowFrom(select: BinaryTree[ast.Select]): BinaryTree[ast.Select] = {
      select match {
        case Leaf(q) => Leaf(q.copy(from = q.from.filter(notSingleRow)))
        case PipeQuery(l, r) => PipeQuery(removeSingleRowFrom(l), r)
        case UnionQuery(l, r) => UnionQuery(removeSingleRowFrom(l), r)
        case UnionAllQuery(l, r) => UnionAllQuery(removeSingleRowFrom(l), r)
        case IntersectQuery(l, r) => IntersectQuery(removeSingleRowFrom(l), r)
        case IntersectAllQuery(l, r) => IntersectAllQuery(removeSingleRowFrom(l), r)
        case MinusQuery(l, r) => MinusQuery(removeSingleRowFrom(l), r)
        case MinusAllQuery(l, r) => MinusAllQuery(removeSingleRowFrom(l), r)
      }
    }

    def notSingleRow(from: TableName): Boolean = {
      from.name != TableName.SingleRow
    }

    def typecheck(
      expr: ast.Expression,
      env: Environment[CT],
      namedExprs: Map[ColumnName, Expr[CT, CV]],
      udfParams: UdfParameters,
      expectedType: Option[CT]
    ): Expr[CT, CV] = {
      val tc = new Typechecker(env, namedExprs, udfParams, userParameters, typeInfo, functionInfo)
      tc(expr, expectedType)
    }

    def verifyAggregatesAndWindowFunctions(stmt: Select[CT, CV]): Unit = {
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
        case WindowedFunctionCall(f, args, partitionBy, orderBy, _context, _start, _end, _exclusion) if allowWindow =>
          args.foreach(verifyAggregatesAndWindowFunctions(_, allowAggregates, false, groupBys))
          partitionBy.foreach(verifyAggregatesAndWindowFunctions(_, allowAggregates, false, groupBys))
          orderBy.foreach { ob => verifyAggregatesAndWindowFunctions(ob.expr, allowAggregates, false, groupBys) }
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
 }
}
