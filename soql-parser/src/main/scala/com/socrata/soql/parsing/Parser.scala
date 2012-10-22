package com.socrata.soql.parsing

import scala.util.parsing.combinator.{Parsers, PackratParsers}
import util.parsing.input.{Position, Reader, NoPosition}

import com.socrata.soql.DatasetContext
import com.socrata.soql.names._
import com.socrata.soql.tokens
import com.socrata.soql.tokens._
import com.socrata.soql.ast
import com.socrata.soql.ast._

class Parser(implicit ctx: DatasetContext) extends Parsers with PackratParsers {
  type Elem = Token

  /*
   *               *************
   *               * ENDPOINTS *
   *               *************
   */
  def selection(soql: String) = parseFull(selectList, soql)
  def expression(soql: String) = parseFull(expr, soql)
  def orderings(soql: String) = parseFull(orderingList, soql)
  def groupBys(soql: String) = parseFull(groupByList, soql)
  def selectStatement(soql: String) = parseFull(fullSelect, soql)
  def limit(soql: String) = parseFull(integer, soql)
  def offset(soql: String) = parseFull(integer, soql)

  private def parseFull[T](parser: Parser[T], soql: String): T = {
    phrase(parser <~ eof)(new LexerReader(soql)) match {
      case Success(result, _) => result
      case Failure(msg, next) => throw new ParseException(msg, next.pos)
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

  def accept[T <: Elem](implicit mfst: ClassManifest[T]): Parser[T] =
    elem(mfst.erasure.getName, mfst.erasure.isInstance _) ^^(_.asInstanceOf[T])

  def eof = EOF() | acceptIf(_ => false)(errors.missingEOF)

  /*
   *               *************************
   *               * FULL SELECT STATEMENT *
   *               *************************
   */

  def fullSelect =
    (SELECT() ~> selectList ~ opt(whereClause) ~ opt(groupByClause) ~ opt(havingClause) ~ opt(orderByClause) ~ opt(limitClause) ~ opt(offsetClause) ^^ {
      case s ~ w ~ gb ~ h ~ ord ~ lim ~ off => Select(s, w, gb, h, ord, lim, off)
    })

  def whereClause = WHERE() ~> expr
  def groupByClause = GROUP() ~ BY() ~> groupByList
  def havingClause = HAVING() ~> expr
  def orderByClause = ORDER() ~ BY() ~> orderingList
  def limitClause = LIMIT() ~> integer
  def offsetClause = OFFSET() ~> integer

  /*
   *               ********************
   *               * COLUMN-SELECTION *
   *               ********************
   */

  def selectList =
    allSystemSelection ~ opt(COMMA() ~> onlyUserStarSelectList) ^^ {
      case star ~ Some(rest) => rest.copy(allSystemExcept = Some(star))
      case star ~ None => Selection(Some(star), None, Seq.empty)
    } |
    onlyUserStarSelectList

  def onlyUserStarSelectList =
    allUserSelection ~ opt(COMMA() ~> expressionSelectList) ^^ {
      case star ~ Some(rest) => rest.copy(allUserExcept = Some(star))
      case star ~ None => Selection(None, Some(star), Seq.empty)
    } |
    expressionSelectList

  def expressionSelectList = rep1sep(namedSelection, COMMA()) ^^ (Selection(None, None, _))

  def allSystemSelection =
    COLONSTAR() ~ opt(selectExceptions(systemIdentifier)) ^^ { case star ~ exceptions => StarSelection(exceptions.getOrElse(Seq.empty)).positionedAt(star.position) }

  def allUserSelection =
    STAR() ~ opt(selectExceptions(userIdentifier)) ^^ { case star ~ exceptions => StarSelection(exceptions.getOrElse(Seq.empty)).positionedAt(star.position) }

  def selectExceptions(identifierType: Parser[(String, Position)]): Parser[Seq[(ColumnName, Position)]] =
    LPAREN() ~ EXCEPT() ~> rep1sep(identifierType, COMMA()) <~ (RPAREN() | failure(errors.missingArg)) ^^ { namePoses =>
      namePoses.map { case (name, pos) =>
        (ColumnName(name), pos)
      }
    }

  def namedSelection = expr ~ opt(AS() ~> userIdentifier) ^^ {
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
    case e ~ Some(order) ~ None => OrderBy(e, order == ASC(), true)
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

  /*
   *               ***************
   *               * EXPRESSIONS *
   *               ***************
   */

  val literal =
    accept[LiteralToken] ^^ {
      case n: tokens.NumberLiteral => ast.NumberLiteral(n.value).positionedAt(n.position)
      case s: tokens.StringLiteral => ast.StringLiteral(s.value).positionedAt(s.position)
      case b: tokens.BooleanLiteral => ast.BooleanLiteral(b.value).positionedAt(b.position)
      case n: NULL => NullLiteral().positionedAt(n.position)
    }


  val userIdentifier: Parser[(String, Position)] =
    accept[tokens.Identifier] ^^ { t =>
      (t.value, t.position)
    } | failure(errors.missingUserIdentifier)

  val systemIdentifier: Parser[(String, Position)] =
    accept[tokens.SystemIdentifier] ^^ { t =>
      (t.value, t.position)
    } | failure(errors.missingSystemIdentifier)

  val identifier: Parser[(String, Position)] = systemIdentifier | userIdentifier | failure(errors.missingIdentifier)

  def paramList: Parser[Either[Position, Seq[Expression]]] =
    // the clauses have to be in this order, or it can't backtrack enough to figure out it's allowed to take
    // the STAR path.
    STAR() ^^ { star => Left(star.position) } |
    repsep(expr, COMMA()) ^^ (Right(_))

  def params: Parser[Either[Position, Seq[Expression]]] =
    LPAREN() ~> paramList <~ (RPAREN() | failure(errors.missingArg))

  def identifier_or_funcall: Parser[Expression] =
    identifier ~ opt(params) ^^ {
      case ((ident, identPos)) ~ None =>
        ColumnOrAliasRef(ColumnName(ident)).positionedAt(identPos)
      case ((ident, identPos)) ~ Some(Right(params)) =>
        FunctionCall(FunctionName(ident), params).positionedAt(identPos).functionNameAt(identPos)
      case ((ident, identPos)) ~ Some(Left(position)) =>
        FunctionCall(SpecialFunctions.StarFunc(ident), Seq.empty).positionedAt(identPos).functionNameAt(identPos)
    }

  def paren: Parser[Expression] =
    LPAREN() ~> expr <~ RPAREN() ^^ { e => FunctionCall(SpecialFunctions.Parens, Seq(e)).positionedAt(e.position).functionNameAt(e.position) }

  def atom =
    literal | identifier_or_funcall | paren | failure(errors.missingExpr)

  lazy val dereference: PackratParser[Expression] =
    dereference ~ DOT() ~ identifier ^^ {
      case a ~ dot ~ ((b, bPos)) =>
        FunctionCall(SpecialFunctions.Subscript, Seq(a, ast.StringLiteral(b).positionedAt(bPos))).
          positionedAt(a.position).
          functionNameAt(dot.position)
    } |
    dereference ~ LBRACKET() ~ expr ~ RBRACKET() ^^ {
      case a ~ lbrak ~ b ~ _ =>
        FunctionCall(SpecialFunctions.Subscript, Seq(a, b)).
          positionedAt(a.position).
          functionNameAt(lbrak.position)
    } |
    atom

  lazy val cast: PackratParser[Expression] =
    cast ~ COLONCOLON() ~ identifier ^^ {
      case a ~ colcol ~ ((b, bPos)) =>
        Cast(a, TypeName(b)).positionedAt(a.position).operatorAndTypeAt(colcol.position, bPos)
    } |
    dereference

  val unary_op =
    MINUS() | PLUS()

  lazy val unary: PackratParser[Expression] =
    opt(unary_op) ~ unary ^^ {
      case None ~ b => b
      case Some(f) ~ b => FunctionCall(SpecialFunctions.Operator(f.printable), Seq(b)).positionedAt(f.position).functionNameAt(f.position)
    } |
    cast

  val factor_op =
    STAR() | SLASH()

  lazy val factor: PackratParser[Expression] =
    opt(factor ~ factor_op) ~ unary ^^ {
      case None ~ a => a
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b)).positionedAt(a.position).functionNameAt(op.position)
    }

  val term_op =
    PLUS() | MINUS() | PIPEPIPE()

  lazy val term: PackratParser[Expression] =
    opt(term ~ term_op) ~ factor ^^ {
      case None ~ a => a
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b)).positionedAt(a.position).functionNameAt(op.position)
    }

  val order_op =
    EQUALS() | LESSGREATER() | LESSTHAN() | LESSTHANOREQUALS() | GREATERTHAN() | GREATERTHANOREQUALS() | EQUALSEQUALS() | BANGEQUALS()

  lazy val order: PackratParser[Expression] =
    opt(order ~ order_op) ~ term ^^ {
      case None ~ a => a
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b)).positionedAt(a.position).functionNameAt(op.position)
    }

  lazy val isBetween: PackratParser[Expression] =
    isBetween ~ IS() ~ NULL() ^^ {
      case a ~ is ~ _ => FunctionCall(SpecialFunctions.IsNull, Seq(a)).positionedAt(a.position).functionNameAt(is.position)
    } |
    isBetween ~ IS() ~ (NOT() | failure(errors.missingKeywords(NOT(), NULL()))) ~ NULL() ^^ {
      case a ~ is ~ not ~ _ =>
        FunctionCall(SpecialFunctions.IsNotNull, Seq(a)).positionedAt(a.position).functionNameAt(is.position)
    } |
    isBetween ~ BETWEEN() ~ isBetween ~ AND() ~ isBetween ^^ {
      case a ~ between ~ b ~ _ ~ c =>
        FunctionCall(SpecialFunctions.Between, Seq(a, b, c)).positionedAt(a.position).functionNameAt(between.position)
    } |
    isBetween ~ (NOT() | failure(errors.missingKeywords(NOT(), BETWEEN()))) ~ BETWEEN() ~ isBetween ~ AND() ~ isBetween ^^ {
      case a ~ not ~ _ ~ b ~ _ ~ c =>
        FunctionCall(SpecialFunctions.NotBetween, Seq(a, b, c)).positionedAt(a.position).functionNameAt(not.position)
    } |
    order

  lazy val negation: PackratParser[Expression] =
    NOT() ~ negation ^^ { case op ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(b)).positionedAt(op.position).functionNameAt(op.position) } |
    isBetween

  lazy val conjunction: PackratParser[Expression] =
    opt(conjunction ~ AND()) ~ negation ^^ {
      case None ~ b => b
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b)).positionedAt(a.position).functionNameAt(op.position)
    }

  lazy val disjunction: PackratParser[Expression] =
    opt(disjunction ~ OR()) ~ conjunction ^^ {
      case None ~ b => b
      case Some(a ~ op) ~ b => FunctionCall(SpecialFunctions.Operator(op.printable), Seq(a, b)).positionedAt(a.position).functionNameAt(op.position)
    }

  def expr = disjunction | failure(errors.missingExpr)
}

object Parser extends App {
  implicit val ctx = new DatasetContext {
    val locale = com.ibm.icu.util.ULocale.ENGLISH
    val columns = com.socrata.collection.OrderedSet.empty[ColumnName]
  }
  def p = new Parser

  def show[T](x: => T) {
    try {
      println(x)
    } catch {
      case l: LexerError =>
        val p = l.position
        println("[" + p.line + "." + p.column + "] failure: "+ l.getClass.getSimpleName)
        println()
        println(p.longString)
    }
  }

  try {
    show(new LexerReader("").toStream.force)
    show(new LexerReader("- -1*a(1,b)*(3)==2 or x between 1 and 3").toStream.force)
    show(p.expression("- -1*a(1,b)*(3)==2 or x not between 1 and 3"))
    show(p.expression("x between a is null and a between 5 and 6 or f(x x"))
    show(p.expression("x between a"))
    show(p.expression("x is"))
    show(p.expression("x is not 5"))
    show(p.expression("x is x"))
    show(p.expression("x is not gnu"))
    show(p.expression("x is not"))
    show(p.expression("x between"))
    show(p.expression("x is not between"))
    show(p.expression("x not 5"))
    show(p.expression("x between"))
    show(p.expression("x not between"))
    show(p.expression("not"))
    show(p.expression("x x"))
    show(p.expression("étäøîn"))
    show(p.expression("_abc + -- hello world!\n123 -- gnu"))
    show(p.expression("\"gnu\""))
    show(p.expression("\"\\U01D000\""))
    show(p.expression("* 8"))
    show(p.expression(""))
    show(p.orderings("a,b desc,c + d/e"))
    show(p.orderings(""))
    show(p.orderings("a,"))
    show(p.orderings("a ascdesc"))
    show(p.selection(""))
    show(p.selection("a,b,c,"))
    show(p.selection("a,b as,c"))
    show(p.selection("a,b as x,c"))
    show(p.selection("a,b as x,c as y"))
    show(p.selection("a,b as x,c as"))
    show(p.selectStatement(""))
    show(p.selectStatement("x"))
    show(p.selectStatement("select"))
    show(p.selectStatement("select a, b where"))
    show(p.selectStatement("select a, b group"))
    show(p.selectStatement("select a, b order"))
    show(p.selectStatement("select a, b having"))
    show(p.selectStatement("select * where x group by y having z order by w"))
    show(p.selectStatement("select *"))
    show(p.selectStatement("select *("))
    show(p.selectStatement("select *(except"))
    show(p.selectStatement("select *(except a"))
    show(p.selectStatement("select *(except a)"))
    show(p.selectStatement("select *(except a,"))
    show(p.selectStatement("select *(except a,b)"))
    show(p.expression("a"))
    show(p.expression("a."))
    show(p.expression("a.b"))
    show(p.expression("a.b."))
    show(p.expression("a[b"))
    show(p.expression("a[b]"))
    show(p.expression("a[b]."))
    show(p.expression("a[b].c"))
    show(p.expression("a[b].c[d]"))
    show(p.orderings("x, y asc null last"))
    show(p.selectStatement("select * order by x, y gnu,"))
    show(p.expression("-a :: b :: c"))
    show(p.selectStatement("select * order by x, y limit 5"))
    show(p.limit("12e1"))
    show(p.selectStatement("SELECT x AS `where`, y AS `hither-thither`"))
    show(p.identifier(new LexerReader("__gnu__")))
    show(p.expression("a(1) || 'blah'"))
    show(p.expression("`hello world`"))
    show(p.selectStatement("select x"))

    show(p.expression("count(__gnu__)+a+b"))
    show(p.expression("count(__gnu__)+a+b").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    show(p.expression("count(a) == 'hello, world!  This is a smiling gnu.'").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    show(p.expression("count(a) == `hello-world`").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    show(p.expression("`-world` + 2").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    show(p.expression("world is not null").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    show(p.expression("1 + `-world` = `a-` - 1").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    show(p.expression(":id - 1").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    show(p.expression("count(a) == 'hello, world!  This is a smiling gnu.'"))
    show(p.selectStatement("select x as :x"))

    show(p.selectStatement("select :*"))
    show(p.selectStatement("select *"))
    show(p.selectStatement("select :*,"))
    show(p.selectStatement("select :*,a"))
    show(p.selectStatement("select *,"))
    show(p.selectStatement("select *,a"))
    show(p.selectStatement("select :*,*"))
    show(p.selectStatement("select :*,*,"))
    show(p.selectStatement("select :*,*,a"))
    show(p.selectStatement("select :*(except a)"))
    show(p.selectStatement("select :*(except :a)"))
    show(p.selectStatement("select *(except :a)"))
    show(p.selectStatement("select *(except a)"))
  } catch {
    case e: Exception =>
      e.printStackTrace()
  }
}

/*

unary_!
* /
+ -
< <= == >= > IS IS_NOT
&&
||

*/
