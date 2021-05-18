package com.socrata.soql.ast

import scala.util.parsing.input.{NoPosition, Position}

import java.util.UUID;

import com.socrata.soql.environment._
import Select._
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery, UnionQuery, UnionAllQuery, IntersectQuery, MinusQuery}

/**
  * All joins must select from another table. A join may also join on sub-select. A join on a sub-select requires an
  * alias, but the alias is optional if the join is on a table-name only (e.g. "join 4x4").
  *
  *   "join 4x4" => JoinSelect(Left(TableName(4x4, None))) { aliasOpt = None }
  *   "join 4x4 as a" => JoinSelect(Left(TableName(4x4, a))) { aliasOpt = a }
  *   "join (select id from 4x4) as a" =>
  *     JoinSelect(Right(SubSelect(List(_select_id_), a))) { aliasOpt = a }
  *   "join (select c.id from 4x4 as c) as a" =>
  *     JoinSelect(Right(SubSelect(List(_select_id_), a))) { aliasOpt = a }
  */
sealed trait JoinSelect {
  val alias: Option[String]
  def allTableNames: Set[String]
  def replaceHoles(f: Hole => Expression): JoinSelect
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): JoinSelect
  def directlyReferencedJoinFuncs: Set[TableName]
}
case class JoinTable(tableName: TableName) extends JoinSelect {
  val alias = tableName.alias
  override def toString = tableName.toString
  def replaceHoles(f: Hole => Expression): this.type = this
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): this.type = this
  def directlyReferencedJoinFuncs = Set.empty

  def allTableNames = {
    val result = Set.newBuilder[String]
    result += tableName.name
    result ++= tableName.alias
    result.result()
  }
}
case class JoinQuery(selects: BinaryTree[Select], definiteAlias: String) extends JoinSelect {
  val alias = Some(definiteAlias)
  override def toString = "(" + Select.toString(selects) + ") AS " + TableName.removeValidPrefix(definiteAlias)
  def replaceHoles(f: Hole => Expression): JoinQuery =
    copy(Select.walkTreeReplacingHoles(selects, f))
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): JoinQuery =
    copy(selects = Select.rewriteJoinFuncs(selects, f)(aliasProvider))
  def directlyReferencedJoinFuncs = Select.findDirectlyReferencedJoinFuncs(selects)

  val allTableNames = Select.allTableNames(selects) + definiteAlias
}
case class JoinFunc(tableName: TableName, params: Seq[Expression])(val position: Position) extends JoinSelect {
  val alias = tableName.alias
  override def toString =
    TableName.withSoqlPrefix(tableName.name) + "(" + params.mkString(", ") + ")" + tableName.aliasWithoutPrefix.map(" AS " + _)

  def allTableNames = {
    val result = Set.newBuilder[String]
    result += tableName.name
    result ++= tableName.alias
    result.result()
  }

  def directlyReferencedJoinFuncs = Set(tableName.copy(alias = None))

  def replaceHoles(f: Hole => Expression): JoinFunc =
    copy(params = params.map(_.replaceHoles(f)))(position)

  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): JoinQuery = {
    val aliasless = tableName.copy(alias=None)
    f.get(aliasless) match {
      case Some(udf) =>
        rewrite(udf, params, aliasProvider).rewriteJoinFuncs(f, aliasProvider)
      case None =>
        throw UnknownUDF(aliasless)(position)
    }
  }

  def rewrite(udf: UDF, params: Seq[Expression], aliasProvider: AliasProvider): JoinQuery = {
    // ok we're going to rewrite
    //   join @foo(x, y, z) as bleh
    // to
    //   join (select @expr.:*, @expr.* from (select x,y,z from @single_row) as vars join lateral (body) as expr on true) as bleh;
    // ..or that's the ideal, anyway.  Turns out you can't FROM a
    // query in soql, so we'll actually rewrite to "..from @single_row
    // join (select x,y,z) join lateral (body)" instead.
    //
    // Probably will try to turn `from single_row join bleh on true`
    // into `from bleh` over in the postgres adapter later.
    if(udf.arguments.length != params.length) {
      throw MismatchedParameterCount(expected = udf.arguments.length, got = params.length)(position)
    }
    val expr = aliasProvider("expr")
    val vars = aliasProvider("vars")
    val singleRowFrom = Some(TableName(TableName.SingleRow))
    val labelledJoin =
      Select(
        distinct = false,
        selection = Selection(Some(StarSelection(Some(expr), Nil)),
                              Seq(StarSelection(Some(expr), Nil)),
                              Nil),
        from = singleRowFrom,
        joins = Seq(
          InnerJoin(
            JoinQuery(
              Leaf(
                Select(
                  distinct = false,
                  selection = Selection(None, Nil,
                                        udf.arguments.zip(params).map {
                                          case ((holeName, typ), expression) =>
                                            SelectedExpression(FunctionCall(SpecialFunctions.Cast(typ), Seq(expression), None)(NoPosition, NoPosition),
                                                               Some((ColumnName(holeName.name), NoPosition)))
                                        }.toList),
                  from = singleRowFrom,
                  joins = Nil, where = None, groupBys = Nil, having = None, orderBys = Nil, limit = None, offset = None, search = None)),
              vars),
            on = BooleanLiteral(true)(NoPosition),
            lateral = false),
          InnerJoin(
            JoinQuery(
              Select.walkTreeReplacingHoles(udf.body, { hole =>
                ColumnOrAliasRef(Some(vars), ColumnName(hole.name.name))(hole.position)
              }),
              expr),
            on = BooleanLiteral(true)(NoPosition),
            lateral = true)),
        where = None, groupBys = Nil, having = None, orderBys = Nil, limit = None, offset = None, search = None)

    JoinQuery(Leaf(labelledJoin), tableName.alias.getOrElse(tableName.name))
  }
}

trait AliasProvider {
  def apply(base: String): String
}

object Select {
  def itrToString(prefix: String, l: Iterable[_], sep: String = " "): Option[String] = {
    itrToString(Some(prefix), l, sep)
  }

  def itrToString(prefix: Option[String], l: Iterable[_], sep: String): Option[String] = {
    if (l.nonEmpty) {
      Some(l.mkString(prefix.map(p => s"$p ").getOrElse(""), sep, ""))
    } else {
      None
    }
  }

  def toString(selects: BinaryTree[Select]): String = {
    selects match {
      case PipeQuery(l, r) =>
        val ls = Select.toString(l)
        val rs = Select.toString(r)
        s"$ls |> $rs"
      case Compound(op, l, r) =>
        val ls = Select.toString(l)
        val rs = Select.toString(r)
        s"$ls $op $rs"
      case Leaf(select) =>
        select.toString
    }
  }

  implicit class StringOptionList(l: List[Option[String]]) {
    def flatString = l.flatten.mkString(" ")
  }

  def allTableNames(node: BinaryTree[Select]): Set[String] = {
    val result = Set.newBuilder[String]
    node.foreach { leaf => result ++= leaf.allTableNames }
    result.result()
  }

  def walkTreeReplacingHoles(node: BinaryTree[Select], f: Hole => Expression): BinaryTree[Select] = {
    node.map(_.replaceHoles(f))
  }

  def findDirectlyReferencedJoinFuncs(node: BinaryTree[Select]): Set[TableName] = {
    node match {
      case c: Compound[Select] => findDirectlyReferencedJoinFuncs(c.left) union findDirectlyReferencedJoinFuncs(c.right)
      case Leaf(select) => select.directlyReferencedJoinFuncs
    }
  }

  def aliasProvider(node: BinaryTree[Select], funcs: Map[TableName, UDF]): AliasProvider = {
    val result = Set.newBuilder[String]
    result ++= allTableNames(node)
    for((tn, udf) <- funcs) {
      result += tn.name
      result ++= tn.alias
      result ++= allTableNames(udf.body)
    }
    val trulyAllTableNames = result.result()
    // ..and now we just find a name that isn't in that set!
    new AliasProvider {
      var next = 0
      def apply(base: String): String = {
        def candidate = "_" + base + "_" + next
        do {
          next += 1
        } while(trulyAllTableNames.contains(candidate))
        candidate
      }
    }
  }

  def rewriteJoinFuncs(node: BinaryTree[Select], funcs: Map[TableName, UDF])(implicit aliasProvider: AliasProvider = Select.aliasProvider(node, funcs)): BinaryTree[Select] =
    node.map(_.rewriteJoinFuncs(funcs))
}

case class UDF(arguments: Seq[(HoleName, TypeName)], body: BinaryTree[Select])

/**
  * Represents a single select statement, not including the from. Top-level selects have an implicit "from"
  * based on the current view. Joins do require a "from" (which is a member of the JoinSelect class). A List[Select]
  * represents chained soql (e.g. "select a, b |> select a"), and is what is returned from a top-level parse of a
  * soql string (see Parser#selectStatement).
  *
  * the chained soql:
  *   "select id, a |> select a"
  * is equivalent to:
  *   "select a from (select id, a [from current view]) as alias"
  * and is represented as:
  *   List(select_id_a, select_id)
  */
case class Select(
  distinct: Boolean,
  selection: Selection,
  from: Option[TableName],
  joins: Seq[Join],
  where: Option[Expression],
  groupBys: Seq[Expression],
  having: Option[Expression],
  orderBys: Seq[OrderBy],
  limit: Option[BigInt],
  offset: Option[BigInt],
  search: Option[String]) {

  def directlyReferencedJoinFuncs: Set[TableName] =
    joins.foldLeft(Set.empty[TableName]) { (acc, join) =>
      acc union join.from.directlyReferencedJoinFuncs
    }

  def allTableNames: Set[String] = {
    val result = Set.newBuilder[String]
    for(tn <- from) {
      result += tn.name
      result ++= tn.alias
    }
    for(join <- joins) {
      result ++= join.from.allTableNames
    }
    result.result()
  }

  def rewriteJoinFuncs(funcs: Map[TableName, UDF])(implicit aliasProvider: AliasProvider): Select =
    Select(distinct,
           selection,
           from,
           joins.map(_.rewriteJoinFuncs(funcs, aliasProvider)),
           where,
           groupBys,
           having,
           orderBys,
           limit,
           offset,
           search)

  def replaceHoles(f: Hole => Expression): Select =
    Select(distinct,
           selection.replaceHoles(f),
           from,
           joins.map(_.replaceHoles(f)),
           where.map(_.replaceHoles(f)),
           groupBys.map(_.replaceHoles(f)),
           having.map(_.replaceHoles(f)),
           orderBys.map(_.replaceHoles(f)),
           limit,
           offset,
           search)

  private def toString(from: Option[TableName]): String = {
    if(AST.pretty) {
      val distinctStr = if (distinct) "DISTINCT " else ""
      val selectStr = Some(s"SELECT $distinctStr$selection")
      val fromStr = this.from.orElse(from).map(t => s"FROM $t")
      val joinsStr = itrToString(None, joins.map(j => s"${j.toString}"), " ")
      val whereStr = itrToString("WHERE", where)
      val groupByStr = itrToString("GROUP BY", groupBys, ", ")
      val havingStr = itrToString("HAVING", having)
      val obStr = itrToString("ORDER BY", orderBys, ", ")
      val searchStr = itrToString("SEARCH", search.map(Expression.escapeString))
      val limitStr = itrToString("LIMIT", limit)
      val offsetStr = itrToString("OFFSET", offset)

      val parts = List(selectStr, fromStr, joinsStr, whereStr, groupByStr, havingStr, obStr, searchStr, limitStr, offsetStr)
      parts.flatString
    } else {
      AST.unpretty(this)
    }
  }

  def toStringWithFrom(fromTable: TableName): String = toString(Some(fromTable))

  def format(from: Option[TableName]): String = {
    val distinctStr = if (distinct) "DISTINCT " else ""
    val selectStr = Some(s"SELECT $distinctStr$selection")
    val fromStr = this.from.orElse(from).map(t => s"\nFROM $t")
    val joinsStr = itrToString(None, joins.map(j => s"\n${j.toString}"), "\n ")
    val whereStr = itrToString("\nWHERE", where)
    val groupByStr = itrToString("\nGROUP BY", groupBys, ", ")
    val havingStr = itrToString("\nHAVING", having)
    val obStr = itrToString("\nORDER BY", orderBys, ", ")
    val searchStr = itrToString("\nSEARCH", search.map(Expression.escapeString))
    val limitStr = itrToString("\nLIMIT", limit)
    val offsetStr = itrToString("\nOFFSET", offset)

    val parts = List(selectStr, fromStr, joinsStr, whereStr, groupByStr, havingStr, obStr, searchStr, limitStr, offsetStr)
    parts.flatString
  }

  override def toString: String = toString(None)
}

// represents the columns being selected. examples:
// "first_name, last_name as last"
// "*"
// "*(except id)"
// ":*"      <-- all columns including system columns (date_created, etc.)
// "sum(count) as s"
case class Selection(allSystemExcept: Option[StarSelection], allUserExcept: Seq[StarSelection], expressions: Seq[SelectedExpression]) {
  override def toString = {
    if(AST.pretty) {
      def star(s: StarSelection, token: String) = {
        val sb = new StringBuilder()
        s.qualifier.foreach { x =>
          sb.append(TableName.withSoqlPrefix(x))
          sb.append(TableName.Field)
        }
        sb.append(token)
        if(s.exceptions.nonEmpty) {
          sb.append(s.exceptions.map(e => e._1).mkString(" (EXCEPT ", ", ", ")"))
        }
        sb.toString
      }
      (allSystemExcept.map(star(_, ":*")) ++ allUserExcept.map(star(_, "*")) ++ expressions.map(_.toString)).mkString(", ")
    } else {
      AST.unpretty(this)
    }
  }

  def isSimple = allSystemExcept.isEmpty && allUserExcept.isEmpty && expressions.isEmpty

  def replaceHoles(f: Hole => Expression) =
    copy(expressions = expressions.map(_.replaceHoles(f)))
}

case class StarSelection(qualifier: Option[String], exceptions: Seq[(ColumnName, Position)]) {
  var starPosition: Position = NoPosition
  def positionedAt(p: Position): this.type = {
    starPosition = p
    this
  }

  // SoQL ASTs ignore positions, so define equals and hashcode to honor that
  override def equals(that: Any) =
    that match {
      case StarSelection(thatQualifier, thoseExceptions) =>
        this.qualifier == thatQualifier && this.exceptions.map(_._1) == thoseExceptions.map(_._1)
      case _ =>
        false
    }
  override def hashCode =
    this.qualifier.hashCode & exceptions.map(_._1).hashCode
}

case class SelectedExpression(expression: Expression, name: Option[(ColumnName, Position)]) {
  override def toString =
    if(AST.pretty) {
      name match {
        case Some(name) => expression + " AS " + name._1
        case None => expression.toString
      }
    } else {
      AST.unpretty(this)
    }

  def replaceHoles(f: Hole => Expression): SelectedExpression =
    copy(expression = expression.replaceHoles(f))

  // SoQL ASTs ignore positions, so define equals and hashcode to honor that
  override def equals(that: Any) =
    that match {
      case SelectedExpression(thatExpression, thatName) =>
        this.expression == thatExpression && this.name.map(_._1) == thatName.map(_._1)
      case _ =>
        false
    }
  override def hashCode =
    expression.hashCode ^ name.map(_._1).hashCode
}

case class OrderBy(expression: Expression, ascending: Boolean, nullLast: Boolean) {
  override def toString =
    if(AST.pretty) {
      expression + (if(ascending) " ASC" else " DESC") + (if(nullLast) " NULL LAST" else " NULL FIRST")
    } else {
      AST.unpretty(this)
    }

  def replaceHoles(f: Hole => Expression) =
    copy(expression = expression.replaceHoles(f))
}

object SimpleSelect {
  /**
    * Simple Select is a select created by a join where a sub-query is not used like "JOIN @aaaa-aaaa"
    */
  def isSimple(selects: Seq[Select]): Boolean = {
    selects.forall(_.selection.isSimple)
  }
}
