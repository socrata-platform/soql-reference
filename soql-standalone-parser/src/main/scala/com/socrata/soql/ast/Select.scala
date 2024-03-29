package com.socrata.soql.ast

import scala.util.parsing.input.{NoPosition, Position}
import scala.collection.immutable.VectorBuilder
import java.util.UUID
import com.socrata.soql.environment._
import Select._
import com.socrata.soql.{BinaryTree, Compound, IntersectQuery, Leaf, MinusQuery, PipeQuery, UnionAllQuery, UnionQuery}
import com.socrata.prettyprint.prelude._
import com.socrata.soql.parsing.standalone_exceptions.BadParse

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
  def collectHoles(f: PartialFunction[Hole, Expression]): JoinSelect
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): JoinSelect
  def directlyReferencedJoinFuncs: Set[TableName]
  def doc: Doc[Nothing]
}
case class JoinTable(tableName: TableName) extends JoinSelect {
  val alias = tableName.alias
  override def toString = tableName.toString
  override def doc = Doc(tableName.toString)
  def replaceHoles(f: Hole => Expression): this.type = this
  def collectHoles(f: PartialFunction[Hole, Expression]): this.type = this
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
  override def toString = "(" + Select.toString(selects) + ") AS @" + TableName.removeValidPrefix(definiteAlias)
  override def doc = Seq(Select.toDoc(selects)).encloseNesting(d"(", d"", d") AS @${TableName.removeValidPrefix(definiteAlias)}")
  def replaceHoles(f: Hole => Expression): JoinQuery =
    copy(Select.walkTreeReplacingHoles(selects, f))
  def collectHoles(f: PartialFunction[Hole, Expression]): JoinQuery =
    copy(Select.walkTreeCollectingHoles(selects, f))
  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): JoinQuery =
    copy(selects = Select.rewriteJoinFuncs(selects, f)(aliasProvider))
  def directlyReferencedJoinFuncs = Select.findDirectlyReferencedJoinFuncs(selects)

  val allTableNames = Select.allTableNames(selects) + definiteAlias
}
case class JoinFunc(tableName: TableName, params: Seq[Expression])(val position: Position) extends JoinSelect {
  val alias = tableName.alias
  override def toString =
    TableName.withSoqlPrefix(tableName.name) + "(" + params.mkString(", ") + ")" + tableName.aliasWithoutPrefix.map(" AS @" + _).getOrElse("")
  override def doc =
    params.map(_.doc).encloseNesting(d"${TableName.withSoqlPrefix(tableName.name)}(",
                                     Doc.Symbols.comma,
                                     d")" ++ tableName.aliasWithoutPrefix.fold(d"") { a => d" AS @$a" })

  def allTableNames = {
    val result = Set.newBuilder[String]
    result += tableName.name
    result ++= tableName.alias
    result.result()
  }

  def directlyReferencedJoinFuncs = Set(tableName.copy(alias = None))

  def replaceHoles(f: Hole => Expression): JoinFunc =
    copy(params = params.map(_.replaceHoles(f)))(position)
  def collectHoles(f: PartialFunction[Hole, Expression]): JoinFunc =
    copy(params = params.map(_.collectHoles(f)))(position)

  def rewriteJoinFuncs(f: Map[TableName, UDF], aliasProvider: AliasProvider): JoinQuery = {
    val aliasless = tableName.copy(alias=None)
    f.get(aliasless) match {
      case Some(udf) =>
        rewrite(udf, params, aliasProvider).rewriteJoinFuncs(f, aliasProvider)
      case None =>
        throw UnknownUDF(aliasless)(position)
    }
  }

  private def rewrite(udf: UDF, params: Seq[Expression], aliasProvider: AliasProvider): JoinQuery = {
    // ok we're going to rewrite
    //   join @foo(x, y, z) as bleh
    // to
    //   join (select @expr.* from (select x,y,z from @single_row) as vars join lateral (body) as expr on true) as bleh;
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
        distinct = Indistinct,
        selection = Selection(None,
                              Seq(StarSelection(Some(expr), Nil)),
                              Nil),
        from = singleRowFrom,
        joins = Seq(
          InnerJoin(
            JoinQuery(
              Leaf(
                Select(
                  distinct = Indistinct,
                  selection = Selection(None, Nil,
                                        udf.arguments.zip(params).map {
                                          case ((holeName, typ), expression) =>
                                            SelectedExpression(FunctionCall(SpecialFunctions.Cast(typ), Seq(expression), None)(NoPosition, NoPosition),
                                                               Some((ColumnName(holeName.name), NoPosition)))
                                        }.toList),
                  from = singleRowFrom,
                  joins = Nil, where = None, groupBys = Nil, having = None, orderBys = Nil, limit = None, offset = None, search = None, hints = Seq.empty)),
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
        where = None, groupBys = Nil, having = None, orderBys = Nil, limit = None, offset = None, search = None, hints = Seq.empty)

    JoinQuery(Leaf(labelledJoin), tableName.alias.getOrElse(tableName.name))
  }
}

trait AliasProvider {
  def apply(base: String): String
}

object Select {
  def itrToString(prefix: String, l: Iterable[_], sep: String = " "): Option[String] = {
    itrToString(Some(prefix), l, sep, None)
  }

  def itrToString(prefix: Option[String], l: Iterable[_], sep: String, suffix: Option[String]): Option[String] = {
    if (l.nonEmpty) {
      Some(l.mkString(prefix.map(p => s"$p ").getOrElse(""), sep, suffix.getOrElse("")))
    } else {
      None
    }
  }

  def toString(selects: BinaryTree[Select]): String = {
    toDoc(selects).layoutPretty(LayoutOptions(pageWidth = PageWidth.Unbounded)).toString
  }

  def toDoc(selects: BinaryTree[Select]): Doc[Nothing] = {
    selects match {
      case PipeQuery(l, r) =>
        Seq(Select.toDoc(l), d"|>", Select.toDoc(r)).sep
      case Compound(op, l, r@Compound(_,_,_)) =>
        Seq(Select.toDoc(l), Doc(op), Seq(Select.toDoc(r)).encloseNesting(d"(", d"", d")")).sep
      case Compound(op, l, r) =>
        Seq(Select.toDoc(l), Doc(op), Select.toDoc(r)).sep
      case Leaf(select) =>
        select.doc
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

  def walkTreeCollectingHoles(node: BinaryTree[Select], f: PartialFunction[Hole, Expression]): BinaryTree[Select] = {
    node.map(_.collectHoles(f))
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
  distinct: Distinctiveness,
  selection: Selection,
  from: Option[TableName],
  joins: Seq[Join],
  where: Option[Expression],
  groupBys: Seq[Expression],
  having: Option[Expression],
  orderBys: Seq[OrderBy],
  limit: Option[BigInt],
  offset: Option[BigInt],
  search: Option[String],
  hints: Seq[Hint]) {

  private def docOneCondition(e: Expression): Doc[Nothing] =
    e match {
      case fc@FunctionCall(SpecialFunctions.Parens | SpecialFunctions.Subscript, _, _, _) => fc.doc
      case fc@FunctionCall(SpecialFunctions.Operator(_), _, _, _) => fc.doc.enclose(d"(", d")")
      case other => other.doc
    }

  private def docCondition(expr: Expression): Doc[Nothing] =
    expr match {
      case fc@FunctionCall(SpecialFunctions.Operator(op@"AND"), params, None, None) if params.length == 2 =>
        val vb = new VectorBuilder[Expression]
        fc.variadizeAssociative(vb)
        val result = vb.result()
        (docOneCondition(result.head) +: result.drop(1).map { e => Doc(op) +#+ docOneCondition(e) }).sep.align
      case other =>
        other.doc
    }

  private def docWithFrom(from: Option[TableName]): Doc[Nothing] = {

    def hintClause =
      if (hints.nonEmpty) {
        Seq(((Doc("HINT(") +: hints.map(_.doc).punctuate(Doc(","))) :+ Doc(")")).sep.hang(2))
      } else {
        Nil
      }

    def distinctClause =
      distinct match {
        case Indistinct => Nil
        case FullyDistinct => Seq(d"DISTINCT")
        case DistinctOn(exprs) =>
          Seq(((Doc("DISTINCT ON(") +: exprs.map(_.doc).punctuate(Doc(","))) :+ Doc(")")).sep.hang(2))
      }

    val selectClause = Seq(
      Seq(d"SELECT"),
      hintClause,
      distinctClause,
      selection.docs.punctuate(d",")
    ).flatten.sep.hang(2)

    def fromJoinClause =
      (this.from.orElse(from), joins) match {
        case (None, Seq()) => None
        case (Some(f), js) => Some((d"FROM ${f.toString}" +: js.map(_.doc)).sep.hang(2))
        case (None, js) => Some(js.map(_.doc).sep.hang(2))
      }

    def whereClause = where.map { w => Seq(d"WHERE", docCondition(w)).sep.hang(2) }
    def groupByClause =
      if(groupBys.nonEmpty) {
        Some((d"GROUP BY" +: groupBys.map(_.doc).punctuate(d",")).sep.hang(2))
      } else {
        None
      }
    def havingClause =
      having.map { h => Seq(d"HAVING", docCondition(h)).sep.hang(2) }
    def orderByClause =
      if(orderBys.nonEmpty) {
        Some((d"ORDER BY" +: orderBys.map(_.doc).punctuate(d",")).sep.hang(2))
      } else {
        None
      }
    def searchClause =
      search.map { s => Seq(d"SEARCH", StringLiteral(s)(NoPosition).doc).sep.hang(2) }
    def limitClause =
      limit.map { l => Seq(d"LIMIT", Doc(l)).sep.hang(2) }
    def offsetClause =
      offset.map { o => Seq(d"OFFSET", Doc(o)).sep.hang(2) }
    Seq[Iterable[Doc[Nothing]]](
      Some(selectClause),
      fromJoinClause,
      whereClause,
      groupByClause,
      havingClause,
      orderByClause,
      searchClause,
      limitClause,
      offsetClause
    ).flatten.sep
  }

  def doc = docWithFrom(None)

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
           search,
           hints)

  def collectHoles(distinct: Distinctiveness, f: PartialFunction[Hole, Expression]): Distinctiveness = {
    distinct match {
      case DistinctOn(exprs) => DistinctOn(exprs.map(_.collectHoles(f)))
      case _ => distinct
    }
  }

  def replaceHoles(distinct: Distinctiveness, f: Hole => Expression): Distinctiveness = {
    distinct match {
      case DistinctOn(exprs) => DistinctOn(exprs.map(_.replaceHoles(f)))
      case _ => distinct
    }
  }

  def replaceHoles(f: Hole => Expression): Select =
    Select(replaceHoles(distinct, f),
           selection.replaceHoles(f),
           from,
           joins.map(_.replaceHoles(f)),
           where.map(_.replaceHoles(f)),
           groupBys.map(_.replaceHoles(f)),
           having.map(_.replaceHoles(f)),
           orderBys.map(_.replaceHoles(f)),
           limit,
           offset,
           search,
           hints)

  def collectHoles(f: PartialFunction[Hole, Expression]): Select =
    Select(collectHoles(distinct, f),
           selection.collectHoles(f),
           from,
           joins.map(_.collectHoles(f)),
           where.map(_.collectHoles(f)),
           groupBys.map(_.collectHoles(f)),
           having.map(_.collectHoles(f)),
           orderBys.map(_.collectHoles(f)),
           limit,
           offset,
           search,
           hints)

  override def toString: String = {
    if(AST.pretty) {
      doc.layoutSmart(LayoutOptions(pageWidth = PageWidth.Unbounded)).toString
    } else {
      AST.unpretty(this)
    }
  }

  def toStringWithFrom(fromTable: TableName): String = {
    docWithFrom(Some(fromTable)).layoutSmart(LayoutOptions(pageWidth = PageWidth.Unbounded)).toString
  }
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
      doc.layoutSmart(LayoutOptions(pageWidth = PageWidth.Unbounded)).toString
    } else {
      AST.unpretty(this)
    }
  }

  def isSimple = allSystemExcept.isEmpty && allUserExcept.isEmpty && expressions.isEmpty

  def replaceHoles(f: Hole => Expression) =
    copy(expressions = expressions.map(_.replaceHoles(f)))
  def collectHoles(f: PartialFunction[Hole, Expression]) =
    copy(expressions = expressions.map(_.collectHoles(f)))

  def doc = docs.punctuate(d",").hsep

  def docs: Seq[Doc[Nothing]] = {
    val docs = Seq.newBuilder[Doc[Nothing]]
    docs ++= allSystemExcept.map(_.doc(":*"))
    docs ++= allUserExcept.map(_.doc("*"))
    docs ++= expressions.map { case SelectedExpression(expr, name) =>
      name match {
        case Some((n, _)) =>
          d"${expr.doc} AS `${n.toString}`"
        case None =>
          expr.doc
      }
    }
    docs.result()
  }
}

case class StarSelection(qualifier: Option[String], exceptions: Seq[(ColumnName, Position)]) {
  var starPosition: Position = NoPosition
  def positionedAt(p: Position): this.type = {
    starPosition = p
    this
  }

  def doc(star: String): Doc[Nothing] = {
    val baseDoc = qualifier.fold(Doc.empty) { q => Doc(TableName.withSoqlPrefix(q)) ++ d"." } ++ Doc(star)
    val exceptionsDoc = if(exceptions.nonEmpty) {
      exceptions.map { case (col, _) => Doc(col.toString) }.encloseHanging(d" (EXCEPT ", Doc.Symbols.comma, d")")
    } else {
      Doc.empty
    }
    baseDoc ++ exceptionsDoc
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
        case Some(name) => s"$expression AS `${name._1}`"
        case None => expression.toString
      }
    } else {
      AST.unpretty(this)
    }

  def replaceHoles(f: Hole => Expression): SelectedExpression =
    copy(expression = expression.replaceHoles(f))
  def collectHoles(f: PartialFunction[Hole, Expression]): SelectedExpression =
    copy(expression = expression.collectHoles(f))

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
      doc.layoutSmart(LayoutOptions(pageWidth = PageWidth.Unbounded)).toString
    } else {
      AST.unpretty(this)
    }

  def replaceHoles(f: Hole => Expression) =
    copy(expression = expression.replaceHoles(f))
  def collectHoles(f: PartialFunction[Hole, Expression]) =
    copy(expression = expression.collectHoles(f))

  def doc = {
    val direction = if(ascending) d"ASC" else d"DESC"
    val nullPlacement = if(nullLast) d"NULL LAST" else d"NULL FIRST"
    expression.doc +#+ direction +#+ nullPlacement
  }

  def removeSyntacticParens = copy(expression = expression.removeSyntacticParens)
}

object SimpleSelect {
  /**
    * Simple Select is a select created by a join where a sub-query is not used like "JOIN @aaaa-aaaa"
    */
  def isSimple(selects: Seq[Select]): Boolean = {
    selects.forall(_.selection.isSimple)
  }
}
