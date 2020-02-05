package com.socrata.soql.parsing

import com.socrata.soql.collection.NonEmptySeq
import com.socrata.soql.collection.SeqHelpers._

import scala.reflect.ClassTag
import scala.util.parsing.combinator.{PackratParsers, Parsers}
import util.parsing.input.Position
import com.socrata.soql.{ast, tokens}
import com.socrata.soql.tokens._
import com.socrata.soql.ast._
import com.socrata.soql.environment.{ColumnName, FunctionName, TypeName, ResourceName}

object AbstractParser {
  class Parameters(val allowJoins: Boolean = true, val systemColumnAliasesAllowed: Set[ColumnName] = Set.empty)
  val defaultParameters = new Parameters()
}

abstract class AbstractParser(parameters: AbstractParser.Parameters = AbstractParser.defaultParameters) extends Parsers with PackratParsers {
  import parameters._

  type Elem = Token

  /*
   *               *************
   *               * ENDPOINTS *
   *               *************
   */
  def selection(soql: String): Selection = parseFull(selectList, soql)
  def joins(soql: String): Seq[Join] = uniqueifySubselects(0, parseFull(joinList, soql))._2
  def expression(soql: String): Expression = parseFull(expr, soql)
  def orderings(soql: String): List[OrderBy] = parseFull(orderingList, soql)
  def groupBys(soql: String): List[Expression] = parseFull(groupByList, soql)

  // produces a NonEmptySeq[Select] with one Select per chained soql statement (chained with "|>").
  // the chained soql:
  //  "select id, a |> select a"
  // is equivalent to:
  //  "select a from (select id, a <from current view>) as alias"
  // and is represented as:
  //  NonEmptySeq(select_id_a, Seq(select_id))
  def selectStatement(soql: String): NonEmptySeq[Select] = uniqueifySubselects(0, parseFull(pipedSelect, soql))._2
  def unchainedSelectStatement(soql: String): Select = uniqueifySubselects(0, parseFull(select, soql))._2 // a select statement without pipes
  def parseJoinSource(soql: String): JoinSource = uniqueifySubselects(0, parseFull(joinSource, soql))._2
  def parseSubselectJoinSource(soql: String): JoinSelect = uniqueifySubselects(0, parseFull(subselectJoinSource, soql))._2
  def limit(soql: String): BigInt = parseFull(integer, soql)
  def offset(soql: String): BigInt = parseFull(integer, soql)
  def search(soql: String): String = parseFull(stringLiteral, soql)

  protected def badParse(msg: String, nextPos: Position): Nothing
  protected def lexer(s: String): AbstractLexer

  private def parseFull[T](parser: Parser[T], soql: String): T = {
    phrase(parser <~ eof)(new LexerReader(lexer(soql))) match {
      case Success(result, _) => result
      case Failure(msg, next) => badParse(msg, next.pos)
      case Error(msg, next) => badParse(msg, next.pos)
    }
  }

  private def uniqueifySubselects(startingFrom: Int, selects: NonEmptySeq[Select]): (Int, NonEmptySeq[Select]) =
    selects.mapAccum(startingFrom) { (i, select) =>
      uniqueifySubselects(i, select)
    }

  private def uniqueifySubselects(startingFrom: Int, joins: Seq[Join]): (Int, Seq[Join]) =
    joins.mapAccum(startingFrom) { (i, join) =>
      uniqueifySubselects(i, join)
    }

  private def uniqueifySubselects(startingFrom: Int, select: Select): (Int, Select) = {
    val (n, joins) = select.joins.mapAccum(startingFrom) { (i, select) =>
      uniqueifySubselects(i, select)
    }
    (n, select.copy(joins = joins))
  }

  private def uniqueifySubselects(startingFrom: Int, js: JoinSelect): (Int, JoinSelect) = {
    val (n, subSelect) = js.subSelect.mapAccum(startingFrom) { (i, select) =>
      uniqueifySubselects(i, select)
    }
    (n + 1, js.copy(subSelect = subSelect, outputTableIdentifier = n))
  }

  private def uniqueifySubselects(startingFrom: Int, joinSource: JoinSource): (Int, JoinSource) =
    joinSource match {
      case jt@JoinTable(_, _, _) => (startingFrom + 1, jt.copy(outputTableIdentifier = startingFrom))
      case js@JoinSelect(_, _, _, _, _) => uniqueifySubselects(startingFrom, js)
    }

  private def uniqueifySubselects(startingFrom: Int, join: Join): (Int, Join) =
    join match {
      case ij@InnerJoin(_, _) =>
        val (n, from) = uniqueifySubselects(startingFrom, ij.from)
        (n, ij.copy(from = from))
      case loj@LeftOuterJoin(_, _) =>
        val (n, from) = uniqueifySubselects(startingFrom, loj.from)
        (n, loj.copy(from = from))
      case roj@RightOuterJoin(_, _) =>
        val (n, from) = uniqueifySubselects(startingFrom, roj.from)
        (n, roj.copy(from = from))
      case foj@FullOuterJoin(_, _) =>
        val (n, from) = uniqueifySubselects(startingFrom, foj.from)
        (n, foj.copy(from = from))
    }

  /*
   *               ******************
   *               * ERROR MESSAGES *
   *               ******************
   */
  object errors { // Error messages.  TO BE MADE AVAILABLE FOR TRANSLATION.
    def missingArg = "Comma or close-parenthesis expected"
    def missingExpr = "Expression expected"
    def missingEOF(t: Token) = "Unexpected token `" + t.printable + "'"
    def missingKeywords(keyword: Token, keywords: Token*) = {
      val allKW = (keyword +: keywords).map(_.printable).map("`" + _ + "'")
      val preOr = allKW.dropRight(1)
      val postOr = allKW.last
      val listified = if(preOr.isEmpty) postOr
                      else if(preOr.lengthCompare(1) == 0) preOr.head + " or " + postOr
                      else preOr.mkString(", ") + ", or " + postOr
      listified + " expected"
    }
    def missingIdentifier = "Identifier expected"
    def missingUserIdentifier = "Non-system identifier expected"
    def missingSystemIdentifier = "System identifier expected"
    def missingInteger = "Integer expected"
  }

  /*
   *               *************
   *               * UTILITIES *
   *               *************
   */

  implicit override def accept(t: Elem): Parser[Elem] =
    acceptIf(_ == t)(_ => errors.missingKeywords(t))

  def accept[T <: Elem](implicit mfst: ClassTag[T]): Parser[T] =
    elem(mfst.runtimeClass.getName, mfst.runtimeClass.isInstance _) ^^(_.asInstanceOf[T])

  val eof = acceptIf(_ == EOF()) { t => errors.missingEOF(t) }

  /*
   *               *************************
   *               * FULL SELECT STATEMENT *
   *               *************************
   */

  def onlyIf[T](b: Boolean)(p: Parser[T]): Parser[T] = p.filter(_ => b)

  def ifCanJoin[T](p: Parser[T]): Parser[Option[T]] = opt(onlyIf(allowJoins)(p))

  def ifCanJoinList[T](p: Parser[List[T]]) = ifCanJoin(p).map(_.getOrElse(Nil))

  val select: Parser[Select] = {
    SELECT() ~> distinct ~ selectList ~ ifCanJoinList(joinList) ~ opt(whereClause) ~
      opt(groupByClause) ~ opt(havingClause) ~ orderByAndSearch ~ limitOffset ^^ {
      case d ~ s ~ j ~ w ~ gb ~ h ~ ((ord, sr)) ~ ((lim, off)) =>
        Select(d, s, j, w, gb.getOrElse(Nil), h, ord, lim, off, sr)
    }
  }

  val pipedSelect: Parser[NonEmptySeq[Select]] = {
    rep1sep(select, QUERYPIPE()) ^^ {
      case h :: tail => NonEmptySeq(h, tail)
      case Nil => throw new Exception("Impossible: rep1sep returned nothing")
    }
  }

  val tableIdentifier: Parser[TableName] =
    accept[tokens.TableIdentifier] ^^ { t =>
      TableName(ResourceName(t.value))(t.position)
    } | failure(errors.missingUserIdentifier)

  val simpleUserIdentifier: Parser[(String, Position)] =
    accept[tokens.Identifier] ^^ { t =>
      (t.value, t.position)
    } | failure(errors.missingUserIdentifier)

  def systemIdentifier: Parser[(Option[TableName], String, Position)] = {
    ifCanJoin(tableIdentifier ~ DOT()) ~ simpleSystemIdentifier ^^ {
      case None ~ sid =>
        (None, sid._1, sid._2)
      case Some(qual ~ _) ~ sid =>
        (Some(qual), sid._1, qual.position)
    }
  }

  val simpleSystemIdentifier: Parser[(String, Position)] =
    accept[tokens.SystemIdentifier] ^^ { t =>
      (t.value, t.position)
    } | failure(errors.missingSystemIdentifier)


  val identifier: Parser[(Option[TableName], String, Position)] = systemIdentifier | userIdentifier | failure(errors.missingIdentifier)

  val simpleIdentifier: Parser[(String, Position)] = simpleSystemIdentifier | simpleUserIdentifier | failure(errors.missingIdentifier)

  val simpleIdToAlias: Parser[TableName] = simpleIdentifier ^^ {
    case (alias, pos) => TableName(ResourceName(alias))(pos)
  }

  val selectFrom: Parser[(TableName, Option[TableName], Select)] = {
    SELECT() ~> distinct ~ selectList ~ (FROM() ~> tableIdentifier) ~ opt(AS() ~> simpleIdToAlias) ~ ifCanJoinList(joinList) ~ opt(whereClause) ~
      opt(groupByClause) ~ opt(havingClause) ~ orderByAndSearch ~ limitOffset ^^ {
      case d ~ s ~ t ~ a ~ j ~ w ~ gb ~ h ~ ((ord, sr)) ~ ((lim, off)) =>
        (t, a, Select(d, s, j, w, gb.getOrElse(Nil), h, ord, lim, off, sr))
    }
  }

  val simpleJoinSource: Parser[JoinTable] =
    tableIdentifier ~ opt(AS() ~> simpleIdToAlias) ^^ {
      case tid ~ alias =>
        JoinTable(tid, alias, 0)
    }

  val subselectJoinSource: Parser[JoinSelect] =
    LPAREN() ~> selectFrom ~ opt(QUERYPIPE() ~> pipedSelect) ~ (RPAREN() ~> AS() ~> simpleIdToAlias) ^^ {
      case ((tn, innerAlias, s)) ~ chainedQueries ~ alias =>
        JoinSelect(tn, innerAlias, NonEmptySeq(s, chainedQueries.map(_.seq).getOrElse(Seq.empty)), alias, 0)
    }

  val joinSource: Parser[JoinSource] = simpleJoinSource | subselectJoinSource

  def joinClause: PackratParser[Join] =
    opt((LEFT() | RIGHT() | FULL()) ~ OUTER()) ~ (JOIN() ~> joinSource) ~ (ON() ~> expr) ^^ {
      case None ~ f ~ e =>
        InnerJoin(f, e)
      case Some(jd) ~ f ~ e =>
        OuterJoin(jd._1, f, e)
    }

  def joinList = rep1(joinClause)

  def distinct: Parser[Boolean] = opt(DISTINCT()) ^^ (_.isDefined)

  def orderByAndSearch: Parser[(List[OrderBy], Option[String])] =
    orderByClause ~ opt(searchClause) ^^ { //backward-compat.  We should prefer putting the search first.
      case ob ~ sr => (ob, sr)
    } |
    opt(searchClause ~ opt(orderByClause)) ^^ {
      case Some(sr ~ ob) => (ob.getOrElse(Nil), Some(sr))
      case None => (Nil, None)
    }

  def limitOffset: Parser[(Option[BigInt], Option[BigInt])] =
    limitClause ~ opt(offsetClause) ^^ {
      case l ~ o => (Some(l), o)
    } |
    opt(offsetClause ~ opt(limitClause)) ^^ {
      case Some(o ~ l) => (l, Some(o))
      case None => (None, None)
    }

  def whereClause = WHERE() ~> expr
  def groupByClause = GROUP() ~ BY() ~> groupByList
  def havingClause = HAVING() ~> expr
  def orderByClause = ORDER() ~ BY() ~> orderingList
  def limitClause = LIMIT() ~> integer
  def offsetClause = OFFSET() ~> integer
  def searchClause = SEARCH() ~> stringLiteral

  /*
   *               ********************
   *               * COLUMN-SELECTION *
   *               ********************
   */

  /**
    * There can be only one [table.]:* but multiple [table.]* because
    * all system column names are always identical and it does not support the same column twice w/o rename.
    */
  def selectList =
    allSystemSelection ~ opt(COMMA() ~> onlyUserStarsSelectList) ^^ {
      case star ~ Some(rest) => rest.copy(allSystemExcept = Some(star))
      case star ~ None => Selection(Some(star), Seq.empty, Seq.empty)
    } |
    onlyUserStarsSelectList

  def onlyUserStarsSelectList =
    allUserSelectionList ~ opt(COMMA() ~> expressionSelectList) ^^ {
      case stars ~ Some(rest) => rest.copy(allUserExcept = stars)
      case stars ~ None => Selection(None, stars, Seq.empty)
    } |
    expressionSelectList

  def expressionSelectList = rep1sep(namedSelection, COMMA()) ^^ (Selection(None, Seq.empty, _))

  def allSystemSelection =
    opt(tableIdentifier ~ DOT()) ~ COLONSTAR() ~ opt(selectExceptions(systemIdentifier)) ^^ {
      case None ~ star ~ exceptions => StarSelection(None, exceptions.getOrElse(Seq.empty)).positionedAt(star.position)
      case Some(qual ~ _) ~ star ~ exceptions => StarSelection(Some(qual), exceptions.getOrElse(Seq.empty)).positionedAt(qual.position)
    }

  def allUserSelectionList = rep1sep(allUserSelection, COMMA()) ^^ ( _.map { star => star } )

  def allUserSelection =
    opt(tableIdentifier ~ DOT()) ~ STAR() ~ opt(selectExceptions(userIdentifier)) ^^ {
      case None ~ star ~ exceptions => StarSelection(None, exceptions.getOrElse(Seq.empty)).positionedAt(star.position)
      case Some(qual ~ _) ~ star ~ exceptions => StarSelection(Some(qual), exceptions.getOrElse(Seq.empty)).positionedAt(qual.position)
    }

  def selectExceptions(identifierType: Parser[(Option[TableName], String, Position)]): Parser[Seq[(ColumnName, Position)]] =
    LPAREN() ~ EXCEPT() ~> rep1sep(identifierType, COMMA()) <~ (RPAREN() | failure(errors.missingArg)) ^^ { namePoses =>
      namePoses.map { case (qual, name, pos) =>
        (ColumnName(name), pos)
      }
    }

  def namedSelection = expr ~ opt(AS() ~> simpleIdentifier) ^^ {
    case e ~ None => SelectedExpression(e, None)
    case e ~ Some((name, pos)) =>
      val columnName = ColumnName(name)
      if (!name.startsWith(":") || parameters.systemColumnAliasesAllowed.contains(columnName)) {
        SelectedExpression(e, Some(columnName, pos))
      } else {
        badParse(s"column alias cannot start with colon - $name", pos)
      }
  }

  /*
   *               *************
   *               * ORDERINGS *
   *               *************
   */

  def orderingList = rep1sep(ordering, COMMA())

  def ordering = expr ~ opt(ascDesc) ~ opt(nullPlacement) ^^ {
    case e ~ None ~ None => OrderBy(e, true, true)
    case e ~ Some(order) ~ None => OrderBy(e, order == ASC(), order == ASC())
    case e ~ None ~ Some(firstLast) => OrderBy(e, true, firstLast == LAST())
    case e ~ Some(order) ~ Some(firstLast) => OrderBy(e, order == ASC(), firstLast == LAST())
  }

  def ascDesc = accept[ASC] | accept[DESC]

  def nullPlacement = NULL() ~> (accept[FIRST] | accept[LAST] | failure(errors.missingKeywords(FIRST(), LAST())))

  /*
   *               *************
   *               * GROUP BYS *
   *               *************
   */

  def groupByList = rep1sep(expr, COMMA())

  /*
   *               ********************
   *               * LIMITS & OFFSETS *
   *               ********************
   */

  val integer =
    accept[IntegerLiteral] ^^ (_.asInt) | failure(errors.missingInteger)

  val stringLiteral =
    accept[tokens.StringLiteral] ^^ (_.value)

  /*
   *               ***************
   *               * EXPRESSIONS *
   *               ***************
   */

  val literal =
    accept[LiteralToken] ^^ {
      case n: tokens.NumberLiteral => ast.NumberLiteral(n.value)(n.position)
      case s: tokens.StringLiteral => ast.StringLiteral(s.value)(s.position)
      case b: tokens.BooleanLiteral => ast.BooleanLiteral(b.value)(b.position)
      case n: NULL => NullLiteral()(n.position)
    }

  def userIdentifier: Parser[(Option[TableName], String, Position)] = {
    ifCanJoin(tableIdentifier ~ DOT()) ~ simpleUserIdentifier ^^ {
      case None ~ uid =>
        (None, uid._1, uid._2)
      case Some(qual ~ _) ~ uid =>
        (Some(qual), uid._1, qual.position)
    }
  }

  def paramList: Parser[Either[Position, Seq[Expression]]] =
    // the clauses have to be in this order, or it can't backtrack enough to figure out it's allowed to take
    // the STAR path.
    STAR() ^^ { star => Left(star.position) } |
    repsep(expr, COMMA()) ^^ (Right(_))

  def params: Parser[Either[Position, Seq[Expression]]] =
    LPAREN() ~> paramList <~ (RPAREN() | failure(errors.missingArg))

  def countDistinctParam: Parser[Expression] =
    LPAREN() ~> DISTINCT() ~> expr <~ (RPAREN() | failure(errors.missingArg))


  def windowFunctionParamList: Parser[Either[Position, Seq[Expression]]] =
    rep1sep(expr, COMMA()) ^^ (Right(_))

  def windowFunctionParams: Parser[Seq[Expression]] = {
    def partitionKey(position: Position) = com.socrata.soql.ast.StringLiteral("partition_by")(position)
    def orderKey(position: Position) =  com.socrata.soql.ast.StringLiteral("order_by")(position)

    LPAREN() ~ opt(PARTITION() ~ BY() ~ windowFunctionParamList) ~ opt(ORDER() ~ BY() ~ windowFunctionParamList) ~ RPAREN() ^^ {
      case (lp ~ Some(_ ~ _ ~ Right(partition)) ~ Some(_ ~ _ ~ Right(order)) ~ _) =>
        mergePartitionOrder(partitionKey(lp.position) +: partition, orderKey(lp.position) +: order)
      case (lp ~ Some(_ ~ _ ~ Right(partition)) ~ None ~ _) =>
        partitionKey(lp.position) +: partition
      case (lp ~ None ~ Some(_ ~ _ ~ Right(order)) ~ _) =>
        orderKey(lp.position) +: order
      case _ => // ( )
        Seq.empty
    } | failure(errors.missingArg)
  }

  def mergePartitionOrder(a: Seq[Expression], b: Seq[Expression]): Seq[Expression] = {
    a ++ b
  }

  def functionWithParams(ident: String, params: Either[Position, Seq[Expression]], pos: Position) =
    params match {
      case Left(_) =>
        FunctionCall(SpecialFunctions.StarFunc(ident), Seq.empty)(pos, pos)
      case Right(params) =>
        FunctionCall(FunctionName(ident), params)(pos, pos)
    }

  def identifier_or_funcall: Parser[Expression] = {
    identifier ~ countDistinctParam ^^ {
      case ((_, ident, identPos)) ~ param =>
        functionWithParams(s"${ident.toLowerCase}_distinct", Right(Seq(param)), identPos)
    } |
    identifier ~ opt(params ~ opt(OVER() ~ windowFunctionParams)) ^^ {
      case ((qual, ident, identPos)) ~ None =>
        ColumnOrAliasRef(qual, ColumnName(ident))(identPos)
      case ((_, ident, identPos)) ~ Some(params ~ None) =>
        functionWithParams(ident, params, identPos)
      case ((_, ident, identPos)) ~ Some(params ~ Some(_ ~ wfParams)) =>
        val innerFc = functionWithParams(ident, params, identPos)
        FunctionCall(SpecialFunctions.WindowFunctionOver, innerFc +: wfParams)(identPos, identPos)
    }
  }

  def paren: Parser[Expression] =
    LPAREN() ~> expr <~ RPAREN() ^^ { e => FunctionCall(SpecialFunctions.Parens, Seq(e))(e.position, e.position) }

  def atom =
    literal | identifier_or_funcall | paren | failure(errors.missingExpr)

  lazy val dereference: PackratParser[Expression] =
    dereference ~ DOT() ~ simpleIdentifier ^^ {
      case a ~ dot ~ ((b, bPos)) =>
        FunctionCall(SpecialFunctions.Subscript, Seq(a, ast.StringLiteral(b)(bPos)))(a.position, dot.position)
    } |
    dereference ~ LBRACKET() ~ expr ~ RBRACKET() ^^ {
      case a ~ lbrak ~ b ~ _ =>
        FunctionCall(SpecialFunctions.Subscript, Seq(a, b))(a.position, lbrak.position)
    } |
    atom

  lazy val cast: PackratParser[Expression] =
    cast ~ COLONCOLON() ~ simpleIdentifier ^^ {
      case a ~ colcol ~ ((b, bPos)) =>
        FunctionCall(SpecialFunctions.Cast(TypeName(b)), Seq(a))(a.position, bPos)
    } |
    dereference

  val unary_op =
    MINUS() | PLUS()

  lazy val unary: PackratParser[Expression] =
    opt(unary_op) ~ unary ^^ {
      case None ~ b => b
      case Some(f) ~ b => FunctionCall(SpecialFunctions.Operator(f.printable), Seq(b))(f.position, f.position)
    } |
    cast

  val exp_op =
    CARET()

  lazy val exp: PackratParser[Expression] =
    unary ~ opt(exp_op ~ exp) ^^ {
        case a ~ None => a
        case a ~ Some(op ~ b) => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b))(a.position, op.position)
      }

  val factor_op =
    STAR() | SLASH() | PERCENT()

  lazy val factor: PackratParser[Expression] =
    opt(factor ~ factor_op) ~ exp ^^ {
      case None ~ a => a
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b))(a.position, op.position)
    }

  val term_op =
    PLUS() | MINUS() | PIPEPIPE()

  lazy val term: PackratParser[Expression] =
    opt(term ~ term_op) ~ factor ^^ {
      case None ~ a => a
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b))(a.position, op.position)
    }

  val order_op =
    EQUALS() | LESSGREATER() | LESSTHAN() | LESSTHANOREQUALS() | GREATERTHAN() | GREATERTHANOREQUALS() | EQUALSEQUALS() | BANGEQUALS()

  lazy val order: PackratParser[Expression] =
    opt(order ~ order_op) ~ term ^^ {
      case None ~ a => a
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b))(a.position, op.position)
    }

  lazy val isLikeBetweenIn: PackratParser[Expression] =
    isLikeBetweenIn ~ IS() ~ NULL() ^^ {
      case a ~ is ~ _ => FunctionCall(SpecialFunctions.IsNull, Seq(a))(a.position, is.position)
    } |
    isLikeBetweenIn ~ IS() ~ (NOT() | failure(errors.missingKeywords(NOT(), NULL()))) ~ NULL() ^^ {
      case a ~ is ~ not ~ _ =>
        FunctionCall(SpecialFunctions.IsNotNull, Seq(a))(a.position, is.position)
    } |
    isLikeBetweenIn ~ LIKE() ~ isLikeBetweenIn ^^ {
      case a ~ like ~ b => FunctionCall(SpecialFunctions.Like, Seq(a, b))(a.position, like.position)
    } |
    isLikeBetweenIn ~ BETWEEN() ~ isLikeBetweenIn ~ AND() ~ isLikeBetweenIn ^^ {
      case a ~ between ~ b ~ _ ~ c =>
        FunctionCall(SpecialFunctions.Between, Seq(a, b, c))(a.position, between.position)
    } |
    isLikeBetweenIn ~ NOT() ~ BETWEEN() ~ isLikeBetweenIn ~ AND() ~ isLikeBetweenIn ^^ {
      case a ~ not ~ _ ~ b ~ _ ~ c =>
        FunctionCall(SpecialFunctions.NotBetween, Seq(a, b, c))(a.position, not.position)
    } |
    isLikeBetweenIn ~ NOT() ~ IN() ~ LPAREN() ~ rep1sep(expr, COMMA()) ~ RPAREN() ^^  {
      case a ~ not ~ _ ~ _ ~ es ~ _ =>
        FunctionCall(SpecialFunctions.NotIn, a +: es)(a.position, not.position)
    } |
    isLikeBetweenIn ~ NOT() ~ (LIKE() | failure(errors.missingKeywords(BETWEEN(), IN(), LIKE()))) ~ isLikeBetweenIn ^^ {
      case a ~ not ~ _ ~ b =>
        FunctionCall(SpecialFunctions.NotLike, Seq(a, b))(a.position, not.position)
    } |
    isLikeBetweenIn ~ (IN() | failure(errors.missingKeywords(NOT(), BETWEEN(), IN(), LIKE()))) ~ LPAREN() ~ rep1sep(expr, COMMA()) ~ RPAREN() ^^  {
      case a ~ in ~ _ ~ es ~ _ => FunctionCall(SpecialFunctions.In, a +: es)(a.position, in.position)
    } |
    order

  lazy val negation: PackratParser[Expression] =
    NOT() ~ negation ^^ { case op ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(b))(op.position, op.position) } |
    isLikeBetweenIn

  lazy val conjunction: PackratParser[Expression] =
    opt(conjunction ~ AND()) ~ negation ^^ {
      case None ~ b => b
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b))(a.position, op.position)
    }

  lazy val disjunction: PackratParser[Expression] =
    opt(disjunction ~ OR()) ~ conjunction ^^ {
      case None ~ b => b
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b))(a.position, op.position)
    }

  def expr = disjunction | failure(errors.missingExpr)
}
