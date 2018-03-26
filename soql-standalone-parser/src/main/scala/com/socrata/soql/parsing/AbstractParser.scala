package com.socrata.soql.parsing

import scala.reflect.ClassTag
import scala.util.parsing.combinator.{PackratParsers, Parsers}
import util.parsing.input.Position
import com.socrata.soql.{ast, tokens}
import com.socrata.soql.tokens._
import com.socrata.soql.ast._
import com.socrata.soql.environment.{ColumnName, FunctionName, TableName, TypeName}

object AbstractParser {
  class Parameters(val allowJoins: Boolean = true)
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
  def joins(soql: String): List[Join] = parseFull(joinList, soql)
  def expression(soql: String): Expression = parseFull(expr, soql)
  def orderings(soql: String): Seq[OrderBy] = parseFull(orderingList, soql)
  def groupBys(soql: String): Seq[Expression] = parseFull(groupByList, soql)
  def selectStatement(soql: String): Seq[Select] = parseFull(pipedSelect, soql)
  def unchainedSelectStatement(soql: String): Select = parseFull(unchainedSelect, soql) // a select statement without pipes or subselects
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

  val eof = new Parser[Unit] {
    def apply(in: Input) =
      if(in.first == EOF())
        Success((), in.rest)
      else
        Failure(errors.missingEOF(in.first), in)
  }

  /*
   *               *************************
   *               * FULL SELECT STATEMENT *
   *               *************************
   */

  def pipedSelect: Parser[Seq[Select]] = rep1sep(unchainedSelect, QUERYPIPE())

  def unchainedSelect: Parser[Select] =
    if(allowJoins) {
      SELECT() ~> distinct ~ selectList ~ opt(FROM() ~> tableIdentifier ~ opt(AS() ~> simpleIdentifier)) ~
        opt(joinList) ~ opt(whereClause) ~ opt(groupByClause) ~ opt(havingClause) ~ orderByAndSearch ~ limitOffset ^^ {
        case d ~ s ~ f ~ j ~ w ~ gb ~ h ~ ((ord, sr)) ~ ((lim, off)) => Select(d, s, f.map(x => TableName(x._1._1, x._2.map(TableName.SodaFountainTableNamePrefix + _._1))), j, w, gb, h, ord, lim, off, sr)
      }
    } else {
      SELECT() ~> distinct ~ selectList ~ opt(FROM() ~> tableIdentifier ~ opt(AS() ~> simpleIdentifier)) ~ opt(whereClause) ~ opt(groupByClause) ~ opt(havingClause) ~ orderByAndSearch ~ limitOffset ^^ {
        case d ~ s ~ f ~ w ~ gb ~ h ~ ((ord, sr)) ~ ((lim, off)) => Select(d, s, f.map(x => TableName(x._1._1, x._2.map(TableName.SodaFountainTableNamePrefix + _._1))), None, w, gb, h, ord, lim, off, sr)
      }
    }

  def distinct: Parser[Boolean] = opt(DISTINCT()) ^^ (_.isDefined)

  def orderByAndSearch: Parser[(Option[Seq[OrderBy]], Option[String])] =
    orderByClause ~ opt(searchClause) ^^ { //backward-compat.  We should prefer putting the search first.
      case ob ~ sr => (Some(ob), sr)
    } |
    opt(searchClause ~ opt(orderByClause)) ^^ {
      case Some(sr ~ ob) => (ob, Some(sr))
      case None => (None, None)
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

  def joinClause: PackratParser[Join] =
    opt((LEFT() | RIGHT() | FULL()) ~ OUTER()) ~ JOIN() ~ tableLike ~ opt(AS() ~> simpleIdentifier) ~ ON() ~ expr ^^ {
      case None ~ j ~ t ~ None ~ o ~ e => InnerJoin(t, None, e)
      case None ~ j ~ t ~ Some((alias, pos)) ~ o ~ e => InnerJoin(t, Some(TableName.SodaFountainTableNamePrefix + alias), e)
      case Some(jd) ~ j ~ t ~ None ~ o ~ e => OuterJoin(jd._1, t, None, e)
      case Some(jd) ~ j ~ t ~ Some((alias, pos)) ~ o ~ e => OuterJoin(jd._1, t, Some(TableName.SodaFountainTableNamePrefix + alias), e)
    }

  def joinList = rep1(joinClause)

  def tableLike: Parser[Seq[Select]] =
    LPAREN() ~ pipedSelect ~ RPAREN() ^^ { case _ ~ x ~ _=> x} |
    tableIdentifier ^^ {
      case (tid: String, _) => Seq(Select(false, Selection(None, Seq.empty, Seq.empty), Some(TableName(tid, None)), None, None, None, None, None, None, None, None))
    }

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
      case Some(qual ~ _) ~ star ~ exceptions => StarSelection(Some(qual._1), exceptions.getOrElse(Seq.empty)).positionedAt(qual._2)
    }

  def allUserSelectionList = rep1sep(allUserSelection, COMMA()) ^^ ( _.map { star => star } )

  def allUserSelection =
    opt(tableIdentifier ~ DOT()) ~ STAR() ~ opt(selectExceptions(userIdentifier)) ^^ {
      case None ~ star ~ exceptions => StarSelection(None, exceptions.getOrElse(Seq.empty)).positionedAt(star.position)
      case Some(qual ~ _) ~ star ~ exceptions => StarSelection(Some(qual._1), exceptions.getOrElse(Seq.empty)).positionedAt(qual._2)
    }

  def selectExceptions(identifierType: Parser[(Option[String], String, Position)]): Parser[Seq[(ColumnName, Position)]] =
    LPAREN() ~ EXCEPT() ~> rep1sep(identifierType, COMMA()) <~ (RPAREN() | failure(errors.missingArg)) ^^ { namePoses =>
      namePoses.map { case (qual, name, pos) =>
        (ColumnName(name), pos)
      }
    }

  def namedSelection = expr ~ opt(AS() ~> simpleUserIdentifier) ^^ {
    case e ~ None => SelectedExpression(e, None)
    case e ~ Some((name, pos)) => SelectedExpression(e, Some((ColumnName(name), pos)))
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

  def userIdentifier: Parser[(Option[String], String, Position)] =
    if(allowJoins) {
      opt(tableIdentifier ~ DOT()) ~ simpleUserIdentifier ^^ {
        case None ~ uid =>
          (None, uid._1, uid._2)
        case Some(qual ~ _) ~ uid =>
          (Some(qual._1), uid._1, qual._2)
      }
    } else {
      simpleUserIdentifier ^^ { uid => (None, uid._1, uid._2) }
    }

  val tableIdentifier: Parser[(String, Position)] =
    accept[tokens.TableIdentifier] ^^ { t =>
      (TableName.SodaFountainTableNamePrefix + t.value.substring(1) /* remove prefix @ */, t.position)
    } | failure(errors.missingUserIdentifier)

  val simpleUserIdentifier: Parser[(String, Position)] =
    accept[tokens.Identifier] ^^ { t =>
      (t.value, t.position)
    } | failure(errors.missingUserIdentifier)

  def systemIdentifier: Parser[(Option[String], String, Position)] =
    if(allowJoins) {
      opt(tableIdentifier ~ DOT()) ~ simpleSystemIdentifier ^^ {
        case None ~ sid =>
          (None, sid._1, sid._2)
        case Some(qual ~ _) ~ sid =>
          (Some(qual._1), sid._1, qual._2)
      }
    } else {
      simpleSystemIdentifier ^^ { sid => (None, sid._1, sid._2) }
    }

  val simpleSystemIdentifier: Parser[(String, Position)] =
    accept[tokens.SystemIdentifier] ^^ { t =>
      (t.value, t.position)
    } | failure(errors.missingSystemIdentifier)

  val identifier: Parser[(Option[String], String, Position)] = systemIdentifier | userIdentifier | failure(errors.missingIdentifier)

  val simpleIdentifier: Parser[(String, Position)] = simpleSystemIdentifier | simpleUserIdentifier | failure(errors.missingIdentifier)

  def paramList: Parser[Either[Position, Seq[Expression]]] =
    // the clauses have to be in this order, or it can't backtrack enough to figure out it's allowed to take
    // the STAR path.
    STAR() ^^ { star => Left(star.position) } |
    repsep(expr, COMMA()) ^^ (Right(_))

  def params: Parser[Either[Position, Seq[Expression]]] =
    LPAREN() ~> paramList <~ (RPAREN() | failure(errors.missingArg))


  def windowFunctionParamList: Parser[Either[Position, Seq[Expression]]] =
    rep1sep(expr, COMMA()) ^^ (Right(_))

  def windowFunctionParams: Parser[Either[Position, Seq[Expression]]] =
    LPAREN() ~> RPAREN() ^^ { _ => Right(Seq.empty) } |
      LPAREN() ~> PARTITION() ~> BY() ~> windowFunctionParamList <~ RPAREN() |
      failure(errors.missingArg)


  def identifier_or_funcall: Parser[Expression] =
    identifier ~ opt(params) ~ opt(OVER() ~ windowFunctionParams) ^^ {
      case ((qual, ident, identPos)) ~ None ~ None =>
        ColumnOrAliasRef(qual, ColumnName(ident))(identPos)
      case ((_, ident, identPos)) ~ Some(Right(params)) ~ None =>
        FunctionCall(FunctionName(ident), params)(identPos, identPos)
      case ((_, ident, identPos)) ~ Some(Left(position)) ~ None =>
        FunctionCall(SpecialFunctions.StarFunc(ident), Seq.empty)(identPos, identPos)
      case ((_, ident, identPos)) ~ Some(Right(params)) ~ Some(wfParams) =>
        val innerFc = FunctionCall(FunctionName(ident), params)(identPos, identPos)
        FunctionCall(SpecialFunctions.WindowFunctionOver, innerFc +: wfParams._2.right.get)(identPos, identPos)
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
