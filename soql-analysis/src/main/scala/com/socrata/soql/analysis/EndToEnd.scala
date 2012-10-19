package com.socrata.soql.analysis

import types._
import com.socrata.soql.names._
import com.socrata.soql.DatasetContext
import com.socrata.soql.ast._
import com.ibm.icu.util.ULocale
import util.parsing.input.Position
import com.socrata.collection.OrderedMap

class EndToEnd(val aliases: OrderedMap[ColumnName, typed.TypedFF[SoQLType]], val columns: Map[ColumnName, SoQLType])(implicit ctx: DatasetContext) extends Typechecker[SoQLType] with SoQLTypeConversions {
  def booleanLiteralType(b: Boolean) = SoQLBoolean

  def stringLiteralType(s: String) = SoQLTextLiteral(s)

  def numberLiteralType(n: BigDecimal) = SoQLNumberLiteral(n)

  def nullLiteralType = SoQLNull

  def isAggregate(function: MonomorphicFunction[SoQLType]) = false

  def functionsWithArity(name: FunctionName, n: Int, position: Position) =
    SoQLFunctions.functionsByNameThenArity.get(name) match {
      case Some(funcsByArity) =>
        funcsByArity.get(n) match {
          case Some(fs) =>
            fs
          case None =>
            throw new NoSuchFunction(name, position)
        }
      case None =>
        throw new NoSuchFunction(name, position)
    }

  def typeFor(name: TypeName, position: Position) =
    SoQLType.typesByName.get(name) match {
      case Some(typ) => typ
      case None => throw new UnknownType(position)
    }

  def getCastFunction(from: SoQLType, to: SoQLType, position: Position) = {
    throw new IncompatibleType(position)
  }
}

object EndToEnd extends App {
  implicit val ctx = new DatasetContext {
    implicit val ctx = this

    val locale = ULocale.ENGLISH

    val columnTypes = com.socrata.collection.OrderedMap(
      ColumnName(":id") -> SoQLNumber,
      ColumnName(":updated_at") -> SoQLFixedTimestamp,
      ColumnName(":created_at") -> SoQLFixedTimestamp,
      ColumnName("name_last") -> SoQLText,
      ColumnName("name_first") -> SoQLText,
      ColumnName("visits") -> SoQLNumber,
      ColumnName("last_visit") -> SoQLFixedTimestamp,
      ColumnName("address") -> SoQLLocation,
      ColumnName("balance") -> SoQLMoney
    )

    def columns = columnTypes.keySet
  }

  println(ctx.columnTypes)

  val query = readLine("Enter a SoQL Level 0 Query> ")

  import com.socrata.soql.parsing.Parser
  val parser = new Parser
  val ast = parser.selectStatement(query)
  val aliasesUntyped = AliasAnalysis(ast.selection)

  println("alias typechecking order: " + aliasesUntyped.evaluationOrder)

  val e2e = aliasesUntyped.evaluationOrder.foldLeft(new EndToEnd(OrderedMap.empty, ctx.columnTypes)) { (e2e, alias) =>
    val r = e2e(aliasesUntyped.expressions(alias))
    new EndToEnd(e2e.aliases + (alias -> r), ctx.columnTypes)
  }
  val aliases = e2e.aliases
  val where = ast.where.map(e2e)
  val groupBys = ast.groupBy.map(_.map(e2e))
  val having = ast.having.map(e2e)
  val orderBys = ast.orderBy.map(_.map { ob => e2e(ob.expression) }).getOrElse(Nil)

  val aggregateChecker = new AggregateChecker[SoQLType]
  val hasAggregates = aggregateChecker(aliases.values.toSeq, where, groupBys, having, orderBys)

  println("Outputs: " + aliases)
  println("where: " + where)
  println("group bys: " + groupBys)
  println("having: " + having)
  println("order bys: " + orderBys)
  println("has aggregates: " + hasAggregates)
}
