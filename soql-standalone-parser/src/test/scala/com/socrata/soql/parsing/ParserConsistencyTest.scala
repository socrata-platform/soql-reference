package com.socrata.soql.parsing

import scala.util.parsing.input.Reader

import org.scalatest.FunSuite
import org.scalatest.MustMatchers

import com.socrata.soql.tokens.Token

class ParserConsistencyTest extends FunSuite with MustMatchers {
  val printTimings = false

  private def timing[T](tag: String)(f: => T): T = {
    if(printTimings) {
      val start = System.nanoTime()
      val result = f
      val end = System.nanoTime()
      println(s"$tag: ${(end - start)/1e6}ms")
      result
    } else {
      f
    }
  }

  def check[T](f: (AbstractParser, String) => T) = { (s: String) =>
    val a = new StandaloneParser()
    val b = new StandaloneCombinatorParser()
    if(printTimings) println(s)
    timing("recursive-descent") { f(a, s) } must equal(timing("combinator") { f(b, s) })
  }

  test("combinators and hand parser match - expressions") {
    val go = check(_.expression(_))

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
    val go = check(_.unchainedSelectStatement(_))

    go("SELECT 1")
    go("SELECT WHERE x = 3")
    go("SELECT :*, * WHERE x = 3")
    go("SELECT :*, *, x+2 as y WHERE x = 3")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat)")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat)")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) desc")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) asc")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) desc nulls first")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) asc nulls last")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) desc nulls first")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) asc nulls last")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) asc nulls last limit 5")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) asc nulls last offset 5")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) asc nulls last limit 5 offset 6")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) asc nulls last offset 5 limit 6")
    go("SELECT :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) search 'gnu'")
    go("SELECT distinct :*, *, x+2 as y WHERE x = 3 group by w, x*3, r having f(gnat) order by f(gnat) search 'gnu'")

    go("SELECT * from @this as bleh join @gnarf(1,2,3) on @gnarf.x = @this.y where true")
    go("SELECT * join @gnarf(1,2,3) on @gnarf.x = y where true")
  }

  test("combinators and hand parser match - full compound statements") {
    val go = check(_.binaryTreeSelect(_))

    go("select 1")
    go("select 1 |> select _1")
    go("select 1 union select x from @aaaa-aaaa")
    go("select 1 union (select x from @aaaa-aaaa intersect select y from @bbbb-bbbb)")
  }
}
