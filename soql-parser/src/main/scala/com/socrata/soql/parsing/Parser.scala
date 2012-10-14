package com.socrata.soql.parsing

import scala.util.parsing.combinator.{Parsers, PackratParsers}
import scala.util.parsing.input.{Reader, NoPosition}

import com.socrata.soql.tokens
import com.socrata.soql.tokens._
import com.socrata.soql.ast
import com.socrata.soql.ast._

class Parser extends Parsers with PackratParsers {
  type Elem = Token

  /*
   *               *************
   *               * ENDPOINTS *
   *               *************
   */
  def selection(soql: String) = phrase(selectList <~ eof)(new LexerReader(soql))
  def expression(soql: String) = phrase(expr <~ eof)(new LexerReader(soql))
  def orderings(soql: String) = phrase(orderingList <~ eof)(new LexerReader(soql))
  def groupBys(soql: String) = phrase(groupByList <~ eof)(new LexerReader(soql))
  def selectStatement(soql: String) = phrase(fullSelect <~ eof)(new LexerReader(soql))
  def limit(soql: String) = phrase(integer <~ eof)(new LexerReader(soql))
  def offset(soql: String) = phrase(integer <~ eof)(new LexerReader(soql))

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

  def operatorize(token: Token): Operator =
    if(token.printable.forall(_.isLetter)) KeywordOperator(token.printable, token.position)
    else SymbolicOperator(token.printable, token.position)

  /*
   *               *************************
   *               * FULL SELECT STATEMENT *
   *               *************************
   */

  def fullSelect =
    (SELECT() ~> selectList ~ opt(whereClause) ~ opt(groupByClause ~ opt(havingClause)) ~ opt(orderByClause) ~ opt(limitClause) ~ opt(offsetClause) ^^ {
      case s ~ w ~ gbh ~ ord ~ lim ~ off => Select(s, w, gbh.map { case gb ~ h => GroupBy(gb,h) }, ord, lim, off)
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
    COLONSTAR() ~ opt(selectExceptions(systemIdentifier)) ^^ { case star ~ exceptions => StarSelection(exceptions.getOrElse(Seq.empty), star.position) }

  def allUserSelection =
    STAR() ~ opt(selectExceptions(userIdentifier)) ^^ { case star ~ exceptions => StarSelection(exceptions.getOrElse(Seq.empty), star.position) }

  def selectExceptions(identifierType: Parser[ast.Identifier]) =
    LPAREN() ~ EXCEPT() ~> rep1sep(identifierType, COMMA()) <~ (RPAREN() | failure(errors.missingArg))

  def namedSelection = expr ~ opt(AS() ~> userIdentifier) ^^ {
    case e ~ n => SelectedExpression(e, n)
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
      case n: tokens.NumberLiteral => ast.NumberLiteral(n.value, n.position)
      case s: tokens.StringLiteral => ast.StringLiteral(s.value, s.position)
      case b: tokens.BooleanLiteral => ast.BooleanLiteral(b.value, b.position)
      case n: NULL => NullLiteral(n.position)
    }


  val userIdentifier =
    accept[tokens.Identifier] ^^ { t =>
      ast.Identifier(t.value, t.quoted, t.position)
    } | failure(errors.missingUserIdentifier)

  val systemIdentifier =
    accept[tokens.SystemIdentifier] ^^ { t =>
      ast.Identifier(t.value, t.quoted, t.position)
    } | failure(errors.missingSystemIdentifier)

  val identifier = systemIdentifier | userIdentifier | failure(errors.missingIdentifier)

  def paramList =
    repsep(expr, COMMA()) ^^ (NormalFunctionParameters(_)) |
    STAR() ^^ { star => StarParameter(star.position) }

  def params =
    LPAREN() ~> paramList <~ (RPAREN() | failure(errors.missingArg))

  def identifier_or_funcall =
    identifier ~ opt(params) ^^ {
      case ident ~ None => ident
      case ident ~ Some(params) => FunctionCall(ident, params, ident.position)
    }

  def paren =
    LPAREN() ~ expr <~ RPAREN() ^^ { case open ~ expr => Paren(expr, open.position) }

  def atom =
    literal | identifier_or_funcall | paren | failure(errors.missingExpr)

  lazy val dereference: PackratParser[Expression] =
    dereference ~ DOT() ~ identifier ^^ {
      case a ~ dot ~ b => Dereference(a, b, a.position)
    } |
    dereference ~ LBRACKET() ~ expr ~ RBRACKET() ^^ {
      case a ~ lbrak ~ b ~ _ => Subscript(a, b, a.position)
    } |
    atom

  lazy val cast: PackratParser[Expression] =
    cast ~ COLONCOLON() ~ identifier ^^ {
      case a ~ colcol ~ b => Cast(a, b, a.position)
    } |
    dereference

  val unary_op =
    MINUS() | PLUS()

  lazy val unary: PackratParser[Expression] =
    opt(unary_op) ~ unary ^^ {
      case None ~ b => b
      case Some(f) ~ b => UnaryOperation(operatorize(f), b, f.position)
    } |
    cast

  val factor_op =
    STAR() | SLASH()

  lazy val factor: PackratParser[Expression] =
    opt(factor ~ factor_op) ~ unary ^^ {
      case None ~ a => a
      case Some(a ~ op) ~ b => BinaryOperation(operatorize(op), a, b, a.position)
    }

  val term_op =
    PLUS() | MINUS() | PIPEPIPE()

  lazy val term: PackratParser[Expression] =
    opt(term ~ term_op) ~ factor ^^ {
      case None ~ a => a
      case Some(a ~ op) ~ b => BinaryOperation(operatorize(op), a, b, a.position)
    }

  val order_op =
    EQUALS() | EQUALSEQUALS() | BANGEQUALS() | LESSGREATER() | LESSTHAN() | LESSTHANOREQUALS() | GREATERTHAN() | GREATERTHANOREQUALS()

  lazy val order: PackratParser[Expression] =
    opt(order ~ order_op) ~ term ^^ {
      case None ~ a => a
      case Some(a ~ op) ~ b => BinaryOperation(operatorize(op), a, b, a.position)
    }

  lazy val isBetween: PackratParser[Expression] =
    isBetween ~ IS() ~ NULL() ^^ { case a ~ _ ~ _ => IsNull(a, false, a.position) } |
    isBetween ~ IS() ~ (NOT() | failure(errors.missingKeywords(NOT(), NULL()))) ~ NULL() ^^ {
      case a ~ _ ~ _ ~ _ => IsNull(a, true, a.position)
    } |
    isBetween ~ BETWEEN() ~ isBetween ~ AND() ~ isBetween ^^ {
      case a ~ _ ~ b ~ _ ~ c => Between(a, false, b, c, a.position)
    } |
    isBetween ~ (NOT() | failure(errors.missingKeywords(NOT(), BETWEEN()))) ~ BETWEEN() ~ isBetween ~ AND() ~ isBetween ^^ {
      case a ~ _ ~ _ ~ b ~ _ ~ c => Between(a, true, b, c, a.position)
    } |
    order

  lazy val negation: PackratParser[Expression] =
    NOT() ~ negation ^^ { case op ~ b => UnaryOperation(operatorize(op), b, op.position) } |
    isBetween

  lazy val conjunction: PackratParser[Expression] =
    opt(conjunction ~ AND()) ~ negation ^^ {
      case None ~ b => b
      case Some(a ~ op) ~ b => BinaryOperation(operatorize(op), a, b, a.position)
    }

  lazy val disjunction: PackratParser[Expression] =
    opt(disjunction ~ OR()) ~ conjunction ^^ {
      case None ~ b => b
      case Some(a ~ op) ~ b => BinaryOperation(operatorize(op), a, b, a.position)
    }

  def expr = disjunction | failure(errors.missingExpr)
}

object Parser extends App {
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
