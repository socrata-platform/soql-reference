package com.socrata.soql.parsing

import scala.util.parsing.input.Reader

import org.scalatest.FunSuite
import org.scalatest.MustMatchers

import com.socrata.soql.tokens.Token

class HandParseTest extends FunSuite with MustMatchers {
  private class HRParser extends HandRolledParser {
    override def lexer(s: String) = new StandaloneLexer(s)

    override def expectedEOF(r: HandRolledParser.Reader) = new Exception("Expected EOF or " + r.alternates.map(_.printable).mkString(",") + "; got " + r.first.printable) with HandRolledParser.ParseException {
      val reader = r
    }
    override def expectedTokens(r: HandRolledParser.Reader, token: Set[HandRolledParser.Tokenlike]) = new Exception("Expected one of " + token.union(r.alternates).map(_.printable).mkString(", ") + ", got " + r.first.printable) with HandRolledParser.ParseException {
      val reader = r
    }
  }

  private def timing[T](tag: String)(f: => T): T = {
    val start = System.nanoTime()
    val result = f
    val end = System.nanoTime()
    println(s"$tag: ${(end - start)/1e6}ms")
    result
  }

  test("combinators and hand parser match - expressions") {
    def go(s: String): Unit = {
      val a = new HRParser()
      val b = new StandaloneParser()
      println(s)
      timing("hand-rolled") { a.expression(s) } must equal(timing("combinator") { b.expression(s) })
    }

    go("1 + 1")
    go("1 + 2 * 3")
    go("1 + (2 * 3)")
    go("(1 + 2) * 3")
    go("x and y and z")
    go("x and y and z or a and b and c")
    go("x or y or z and a or b or c")
    go("f()")
    go("f(foo)")
    go("f(foo, bar, gnu(a,b), c)")
    go("count(distinct foo)")
    go("f() over ()")
    go("f() over (partition by x,y)")
    go("f() over (order by x desc,y, z desc nulls last, a desc null first, b, c nulls first)")
    go("x and y and z")
    go("x in (1,2,3)")
    go("x not in (1,2,3)")
    go("x between 1 and 3")
    go("x not between 1 and 3")
    go("x like 'hello'")
    go("x not like 'hello'")
    go("x is null or 5")
    go("x is not null or 5")
    go("-1")
    go("1 + -2")
    go("-2 + 1")
    go("1 * -2")
    go("-2 * 1")
    go("-2 * 1^2")
    go("-2^4 * 1^2")
    go("case when a then b when c then d when e then f else g end")
    go("case when a then b when c then d when e then f end")
  }

  test("combinators and hand parser match - statements") {
    def go(s: String): Unit = {
      val a = new HRParser()
      val b = new StandaloneParser()
      println(s)
      timing("hand-rolled") { a.unchainedSelectStatement(s) } must equal(timing("combinator") { b.unchainedSelectStatement(s) })
    }

    go("SELECT 1")
    go("SELECT WHERE x = 3")
    go("SELECT :*, * WHERE x = 3")
    go("SELECT :*, * WHERE x = 3 GROUP BY q HAVING z")
  }
}
