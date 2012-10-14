package com.socrata.soql.parsing

import scala.util.parsing.input.NoPosition

import org.scalatest._
import org.scalatest.matchers.MustMatchers

import com.socrata.soql.ast._

class ParserTest extends WordSpec with MustMatchers {
  def parseExpression(soql: String) = {
    val p = new Parser
    p.expression(soql) match {
      case p.Success(r, _) => r
      case p.Failure(msg, _) => fail("Unexpected parse failure: " + msg)
    }
  }

  def expectFailure(expectedMsg: String, soql: String) = {
    val p = new Parser
    p.expression(soql) match {
      case p.Success(_, _) => fail("Unexpected success")
      case p.Failure(msg, _) => msg must equal (expectedMsg)
    }
  }

  def ident(name: String) = Identifier(name, false, NoPosition)

  "Parsing" should {
    "require a full `between' clause" in {
      expectFailure("Expression expected", "x between")
      expectFailure("`AND' expected", "x between a")
      expectFailure("Expression expected", "x between a and")
      expectFailure("`BETWEEN' expected", "x not")
      expectFailure("Expression expected", "x not between")
      expectFailure("`AND' expected", "x not between a")
    }

    "require a full `is null' clause" in {
      expectFailure("`NOT' or `NULL' expected", "x is")
      expectFailure("`NULL' expected", "x is not")
      expectFailure("`NULL' expected", "x is not 5")
      expectFailure("`NOT' or `NULL' expected", "x is 5")
    }

    "require an expression after `not'" in {
      expectFailure("Expression expected", "not")
    }

    "reject a more-than-complete expression" in {
      expectFailure("Unexpected token `y'", "x y")
    }

    "reject a null expression" in {
      expectFailure("Expression expected", "")
    }

    "accept a lone identifier" in {
      parseExpression("a").unpositioned must equal (ident("a"))
    }

    "require something after a dereference-dot" in {
      expectFailure("Identifier expected", "a.")
    }

    "accept expr.identifier" in {
      parseExpression("a.b").unpositioned must equal (Dereference(ident("a"), ident("b"), NoPosition))
    }

    "reject expr.identifier." in {
      expectFailure("Identifier expected", "a.b.")
    }

    "reject expr[" in {
      expectFailure("Expression expected", "a[")
    }

    "reject expr[expr" in {
      expectFailure("`]' expected", "a[2 * b")
    }

    "accept expr[expr]" in {
      parseExpression("a[2 * b]").unpositioned must equal (Subscript(ident("a"), BinaryOperation(SymbolicOperator("*", NoPosition), NumberLiteral(2, NoPosition), ident("b"), NoPosition), NoPosition))
    }

    "reject expr[expr]." in {
      expectFailure("Identifier expected", "a[2 * b].")
    }

    "accept expr[expr].ident" in {
      parseExpression("a[2 * b].c").unpositioned must equal (Dereference(Subscript(ident("a"), BinaryOperation(SymbolicOperator("*", NoPosition), NumberLiteral(2, NoPosition), ident("b"), NoPosition), NoPosition), ident("c"), NoPosition))
    }

    "accept expr[expr].ident[expr]" in {
      parseExpression("a[2 * b].c[3]").unpositioned must equal (Subscript(Dereference(Subscript(ident("a"), BinaryOperation(SymbolicOperator("*", NoPosition), NumberLiteral(2, NoPosition), ident("b"), NoPosition), NoPosition), ident("c"), NoPosition), NumberLiteral(3, NoPosition), NoPosition))
    }

  // def show[T](x: => T) {
  //   try {
  //     println(x)
  //   } catch {
  //     case l: LexerError =>
  //       val p = l.position
  //       println("[" + p.line + "." + p.column + "] failure: "+ l.getClass.getSimpleName)
  //       println()
  //       println(p.longString)
  //   }
  // }

  //   show(new LexerReader("").toStream.force)
  //   show(new LexerReader("- -1*a(1,b)*(3)==2 or x between 1 and 3").toStream.force)
  //   show(p.expression("- -1*a(1,b)*(3)==2 or x not between 1 and 3"))
  //   show(p.expression("x between a is null and a between 5 and 6 or f(x x"))
  //   show(p.expression("x x"))

  //   LEXER TESTS
  //   show(p.expression("étäøîn"))
  //   show(p.expression("_abc + -- hello world!\n123 -- gnu"))
  //   show(p.expression("\"gnu\""))
  //   show(p.expression("\"\\U01D000\"")) 
  //   show(p.selectStatement("SELECT x AS `where`, y AS `hither-thither`")) // this one's only a semi-lexer-test
  //   show(p.limit("12e1"))
  //   show(p.expression("`hello world`"))
  //   show(p.identifier("__gnu__"))

  //   show(p.expression("* 8"))
  //   show(p.orderings("a,b desc,c + d/e"))
  //   show(p.orderings(""))
  //   show(p.orderings("a,"))
  //   show(p.orderings("a ascdesc"))
  //   show(p.selection(""))
  //   show(p.selection("a,b,c,"))
  //   show(p.selection("a,b as,c"))
  //   show(p.selection("a,b as x,c"))
  //   show(p.selection("a,b as x,c as y"))
  //   show(p.selection("a,b as x,c as"))
  //   show(p.selectStatement(""))
  //   show(p.selectStatement("x"))
  //   show(p.selectStatement("select"))
  //   show(p.selectStatement("select a, b where"))
  //   show(p.selectStatement("select a, b group"))
  //   show(p.selectStatement("select a, b order"))
  //   show(p.selectStatement("select a, b having"))
  //   show(p.selectStatement("select * where x group by y having z order by w"))
  //   show(p.selectStatement("select *"))
  //   show(p.selectStatement("select *("))
  //   show(p.selectStatement("select *(except"))
  //   show(p.selectStatement("select *(except a"))
  //   show(p.selectStatement("select *(except a)"))
  //   show(p.selectStatement("select *(except a,"))
  //   show(p.selectStatement("select *(except a,b)"))
  //   show(p.orderings("x, y asc null last"))
  //   show(p.selectStatement("select * order by x, y gnu,"))
  //   show(p.expression("-a :: b :: c"))
  //   show(p.selectStatement("select * order by x, y limit 5"))
  //   show(p.expression("a(1) || 'blah'"))
  //   show(p.selectStatement("select x"))

  //   show(p.expression("count(__gnu__)+a+b").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
  //   show(p.expression("count(a) == 'hello, world!  This is a smiling gnu.'").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
  //   show(p.expression("count(a) == `hello-world`").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
  //   show(p.expression("`-world` + 2").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
  //   show(p.expression("world is not null").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
  //   show(p.expression("1 + `-world` = `a-` - 1").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
  //   show(p.expression(":id - 1").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
  //   show(p.expression("count(a) == 'hello, world!  This is a smiling gnu.'"))
  //   show(p.selectStatement("select x as :x"))

  //   show(p.selectStatement("select :*"))
  //   show(p.selectStatement("select *"))
  //   show(p.selectStatement("select :*,"))
  //   show(p.selectStatement("select :*,a"))
  //   show(p.selectStatement("select *,"))
  //   show(p.selectStatement("select *,a"))
  //   show(p.selectStatement("select :*,*"))
  //   show(p.selectStatement("select :*,*,"))
  //   show(p.selectStatement("select :*,*,a"))
  //   show(p.selectStatement("select :*(except a)"))
  //   show(p.selectStatement("select :*(except :a)"))
  //   show(p.selectStatement("select *(except :a)"))
  //   show(p.selectStatement("select *(except a)"))
  }
}
