package com.socrata.soql.analyzer2

import scala.util.parsing.input.Position

import com.socrata.soql.{BinaryTree, Leaf, TrueOp, PipeQuery}
import com.socrata.soql.ast
import com.socrata.soql.collection.{OrderedMap, OrderedSet}
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName, UntypedDatasetContext}
import com.socrata.soql.typechecker.TypeInfo
import com.socrata.soql.aliases.AliasAnalysis

class SoQLAnalysis {
  // This needs at least:
  // a Statement
}

class SoQLAnalyzer[RNS, CT, CV](typeInfo: TypeInfo[CT, CV]) {
  type ScopedResourceName = (RNS, ResourceName)
  type TableMap = com.socrata.soql.analyzer2.TableMap[RNS, CT]
  type FoundTables = com.socrata.soql.analyzer2.FoundTables[RNS, CT]
  type ParsedTableDescription = com.socrata.soql.analyzer2.ParsedTableDescription[RNS, CT]

  def apply(start: FoundTables): SoQLAnalysis = {
    val state = new State(start.tableMap)

    // start.query match {
    //   case FoundTables.Saved(rn) =>
    //     state.analyze(start.tableMap.find(start.initialScope, rn)).finish
    //   case FoundTables.InContext(rn, q) =>
    //     val context = state.analyzeForContext(start.tableMap.find(start.initialScope, rn))
    //     state.analyzeInContext(start.initialScope, Some(context), q).finish
    //   case FoundTables.Standalone(q) =>
    //     state.analyzeInContext(start.initialScope, None, q).finish
    // }

    ???
  }

  private class State(tableMap: TableMap) {
    val labelProvider = new LabelProvider

    def tableFunctionInIncorrectPosition() = ???

    def analyze(desc: ParsedTableDescription, env: Environment[CT]): Statement[CT, CV] = {
      intoStatement(analyzeForContext(desc, env))
    }

    class AddScopeErrorEx(ase: AddScopeError, position: Position) extends Exception

    def intoStatement(from: AtomicFrom[CT, CV]): Statement[CT, CV] = {
      val selectList =
        from match {
          case from: FromTable[CT] =>
            from.columns.map { case (label, NameEntry(name, typ)) =>
              labelProvider.columnLabel() -> NamedExpr(Column(from.label, label, typ), name)
            }
          case from: FromSingleRow =>
            OrderedMap.empty[ColumnLabel, NamedExpr[CT, CV, Windowed]]
          case from: FromStatement[CT, CV] =>
            // Just short-circuit it and return the underlying Statement
            return from.statement
        }

      Select(
        selectList = selectList,
        from = from,
        where = None,
        groupBy = Nil,
        having = None,
        orderBy = Nil,
        limit = None,
        offset = None
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
        case ParsedTableDescription.Query(scope, basedOn, parsed, parameters) =>
          // so this is basedOn |> parsed
          // so we want to analyze "basedOn" in the env we're given
          // and then parsed in that env extended with the result of basedOn
          val from = analyzeForContext(tableMap.find(scope, basedOn), env)
          analyzeInContext(scope, parsed, Some(from), env)
        case ParsedTableDescription.TableFunction(_, _, _) =>
          tableFunctionInIncorrectPosition()
      }
    }

    def analyzeInContext(scope: RNS, q: BinaryTree[ast.Select], from0: Option[AtomicFrom[CT, CV]], env: Environment[CT]): AtomicFrom[CT, CV] = {
      q match {
        case Leaf(select) =>
          analyzeSelection(scope, select, from0, env)
        case PipeQuery(left, right) =>
          val newInput = analyzeInContext(scope, left, from0, env)
          analyzeInContext(scope, right, Some(newInput), env)
        case other: TrueOp[ast.Select] =>
          val lhs = analyzeInContext(scope, other.left, from0, env)
          val rhs = analyzeInContext(scope, other.right, None, env)
          ???
      }
    }

    def analyzeSelection(scope: RNS, select: ast.Select, from0: Option[AtomicFrom[CT, CV]], env: Environment[CT]): FromStatement[CT, CV] = {
      val ast.Select(
        _distinct,
        selection,
        from,
        joins,
        _where,
        _groupBys,
        _having,
        _orderBys,
        _limit,
        _offset,
        _search,
        _hints
      ) = select

      // ok, first things first: establish the schema we're operating
      // in, which means rolling up "from" and "joins" into a From

      val completeFrom = queryInputSchema(scope, from0, from, joins, env)

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

      case class EvaluationState(env: Environment[CT]) {
        def update(name: ColumnName, expr: Expr[CT, CV, Windowed]): EvaluationState = ???
      }

      val finalState = aliasAnalysis.evaluationOrder.foldLeft(EvaluationState(localEnv)) { (state, colName) =>
        val expression = aliasAnalysis.expressions(colName)
        val typed = typecheck(expression, state.env)
        state.update(colName, typed)
      }

      ???
    }

    def collectNamesForAnalysis(from: From[CT, CV]): AliasAnalysis.AnalysisContext = {
      def contextFrom(af: AtomicFrom[CT, CV]): UntypedDatasetContext =
        af match {
          case _: FromSingleRow => new UntypedDatasetContext {
            val columns = OrderedSet.empty
          }
          case t: FromTable[CT] => new UntypedDatasetContext {
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

    def queryInputSchema(scope: RNS, input: Option[AtomicFrom[CT, CV]], from: Option[TableName], joins: Seq[ast.Join], enclosingEnv: Environment[CT]): From[CT, CV] = {
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
          ???
        case (Some(prev), Some(tn@TableName(TableName.This, _))) =>
          // chained query: {something} |> select ... from @this [as alias]
          prev.reAlias(Some(ResourceName(tn.aliasWithoutPrefix.getOrElse(tn.nameWithoutPrefix))))
        case (Some(_), Some(_)) =>
          // chained query: {something} |> select ... from somethingThatIsNotThis
          // this is an error
          ???
        case (None, Some(tn@TableName(TableName.This, _))) =>
          // standalone query: select ... from @this.  But it's standalone, so this is an error
          ???
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
            analyzeJoinSelect(join.from, augmentedFrom)
          } else {
            analyzeJoinSelect(join.from, enclosingEnv)
          }

        val checkedOn = typecheck(join.on, envify(checkedFrom.addToEnvironment(augmentedFrom)))

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
        case Left(e) => throw new AddScopeErrorEx(e, ???)
      }

    def analyzeJoinSelect(js: ast.JoinSelect, env: Environment[CT]): AtomicFrom[CT, CV] = ???
    def typecheck[Ctx <: Windowed](expr: ast.Expression, env: Environment[CT]): Expr[CT, CV, Ctx] = ???
    def expectedBoolean(expr: ast.Expression, got: CT): Nothing = ???
  }
}
