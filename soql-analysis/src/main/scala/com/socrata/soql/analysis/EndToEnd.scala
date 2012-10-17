package com.socrata.soql.analysis

import types._
import com.socrata.soql.names._
import com.socrata.soql.DatasetContext
import com.socrata.soql.ast._
import com.ibm.icu.util.ULocale
import util.parsing.input.Position

class EndToEnd(val aliases: Map[ColumnName, typed.TypedFF[SoQLType]], val columns: Map[ColumnName, SoQLType])(implicit ctx: DatasetContext) extends Typechecker[SoQLType] with SoQLTypeConversions {
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
      ColumnName(":created_at") -> SoQLFloatingTimestamp,
      ColumnName("name_last") -> SoQLText,
      ColumnName("name_first") -> SoQLText,
      ColumnName("visits") -> SoQLNumber,
      ColumnName("last_visit") -> SoQLFixedTimestamp,
      ColumnName("address") -> SoQLLocation
    )

    def columns = columnTypes.keySet
  }

  val query = "select :*, nf || ' ' || name_last as name, name_first as nf, name_last as nl, address.latitude where last_visit > '2012-05-05T00:00:00Z'"

  println(ctx.columnTypes)
  println(query)
  println(SoQLFunctions.functionsByNameThenArity)

  import com.socrata.soql.parsing.Parser
  val parser = new Parser
  val ast = parser.selectStatement(query) match {
    case parser.Success(ast, _) => ast
    case parser.Failure(msg, _) => sys.error(msg)
  }
  val aliasesUntyped = AliasAnalysis(ast.selection)

  println(aliasesUntyped.evaluationOrder)

  val e2e = aliasesUntyped.evaluationOrder.foldLeft(new EndToEnd(Map.empty, ctx.columnTypes)) { (e2e, alias) =>
    val r = e2e(aliasesUntyped.expressions(alias))
    new EndToEnd(e2e.aliases + (alias -> r), ctx.columnTypes)
  }
  println(e2e.aliases)
  println(e2e(ast.where.get))
}
