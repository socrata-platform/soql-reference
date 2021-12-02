package com.socrata.soql.parsing

import com.socrata.NonEmptySeq
import org.scalatest.{FunSpec, MustMatchers}

import com.socrata.soql.ast.Select

class ToStringTest extends FunSpec with MustMatchers {
  val parser = new StandaloneParser()

  describe("expressions") {
    it("simple expressions") {
      val expected = "foo(`hi`)"
      val rendered = parser.expression(expected).toString
      rendered must equal(expected)
    }

    it("nested operators") {
      val betweenExpect = "foo(`something` BETWEEN 1 AND 5)"
      val betweenActual = parser.expression(betweenExpect).toString
      betweenActual must equal(betweenExpect)

      val notNullExpect = "foo(`something` IS NOT NULL)"
      val notNullActual = parser.expression(notNullExpect).toString
      notNullActual must equal(notNullExpect)
    }

    it("multiple unary operators") {
      val expected = "- -`x`"
      val rendered = parser.expression(expected).toString
      rendered must equal(expected)
    }

    it("wide expressions") {
      val expected =
        """foo(bar(baz(
          |  `hello`,
          |  `there`,
          |  'yikes',
          |  'wow this is a complicated function',
          |  'the arglist is very long in terms of rendered width',
          |  'as well as arg count',
          |  'this is a long arglist so it will get broken up',
          |  biz(1, 2, 3, 'this', 'wont'),
          |  this(
          |    'should',
          |    'get broken up',
          |    'becuase it is very long',
          |    'and this much stuff on one line',
          |    'will make things',
          |    'very hard to read',
          |    'i could maybe write a whole poem',
          |    'in these tests',
          |    'but it is 5 oclock',
          |    'and i am not creative',
          |    'sorry about that',
          |    'happy monday'
          |  )
          |)))""".stripMargin
      val rendered = parser.expression(expected).toString
      rendered must equal(expected)
    }

    it("nested expressions ((with extra parens))") {
      val expected =
        """case(
          |  (`species` = 'cat'),
          |  case(
          |    ((`breed` = 'tabby')),
          |    'tabby cat',
          |    `breed` = 'sphinx',
          |    'sphinx cat',
          |    `breed` = 'fuzzy',
          |    'fuzzy cat'
          |  ),
          |  `species` = 'dog',
          |  case(
          |    `breed` = 'corgi',
          |    'corgis!',
          |    `breed` = 'greyhound',
          |    'greyhounds!',
          |    `breed` = 'husky',
          |    'huskys!',
          |    (`breed` == 'shepherd'),
          |    case(
          |      `subbreed` = 'aussie',
          |      'australian shepherd',
          |      `subbreed` = 'german',
          |      'german shepherd',
          |      `subbreed` = 'border',
          |      'border collie',
          |      `subbreed` = 'belgian',
          |      'belgian shepherd',
          |      (`subbreed` = 'pyrenees'),
          |      'great pyrenees'
          |    )
          |  ),
          |  TRUE,
          |  'other'
          |)""".stripMargin
      val rendered = parser.expression(expected).toString
      rendered must equal(expected)
    }

    it("function call with filter and window") {
      val expecteds = Seq(
        "sum(`c1`)",
        "sum(`c1`) FILTER (WHERE TRUE)",
        "sum(`c1`) OVER ()",
        "sum(`c1`) FILTER (WHERE TRUE) OVER ()",
        "sum(`c1`, `c2`)",
        "sum(`c1`, `c2`) FILTER (WHERE TRUE)",
        "sum(`c1`, `c2`) OVER ()",
        "sum(`c1`, `c2`) FILTER (WHERE TRUE) OVER ()",
        "count(`c1`)",
        "count(`c1`) FILTER (WHERE TRUE)",
        "count(`c1`) OVER ()",
        "count(`c1`) FILTER (WHERE TRUE) OVER ()",
        "count(*)",
        "count(*) FILTER (WHERE TRUE)",
        "count(*) OVER ()",
        "count(DISTINCT `c`)",
        "count(DISTINCT `c`) FILTER (WHERE TRUE)",
        "count(DISTINCT `c`) OVER ()",
        "count(DISTINCT `c`) FILTER (WHERE TRUE) OVER ()"
      )
      expecteds.foreach { expected =>
        val parsed = parser.expression(expected)
        val rendered = parsed.toString
        rendered must equal(expected)
      }
    }
  }

  describe("non-join") {
    it("complex") {
      val query =
        """select :id, balance as amt, visits |>
          |  select :id as i, sum(amt)
          |  where visits > 0
          |  group by i, visits
          |  having sum_amt < 5
          |  order by i desc,
          |           sum(amt) null first
          |  search 'gnu'
          |  limit 5
          |  offset 10""".stripMargin
      val expected1 = "SELECT `:id`, `balance` AS `amt`, `visits`"
      val expected2 = "SELECT `:id` AS `i`, sum(`amt`) WHERE `visits` > 0 GROUP BY `i`, `visits` HAVING `sum_amt` < 5 ORDER BY `i` DESC NULL FIRST, sum(`amt`) ASC NULL FIRST SEARCH 'gnu' LIMIT 5 OFFSET 10"
      val parsed = parser.selectStatement(query).map(_.toString)
      parsed must equal(NonEmptySeq(expected1, List(expected2)))
    }
  }

  describe("joins") {
    it("simple") {
      val query = "select @aaaa-aaaa.name_last join @aaaa-aaaa on name_last = @aaaa-aaaa.name_last"
      val expected = "SELECT @aaaa-aaaa.`name_last` JOIN @aaaa-aaaa ON `name_last` = @aaaa-aaaa.`name_last`"
      val parsed = parser.selectStatement(query).map(_.toString)
      parsed must equal(NonEmptySeq(expected))
    }

    it("simple 2") {
      val query = "select :id, @a.name_last join @aaaa-aaaa as a on name_last = @a.name_last"
      val expected = "SELECT `:id`, @a.`name_last` JOIN @aaaa-aaaa AS @a ON `name_last` = @a.`name_last`"
      val parsed = parser.selectStatement(query).map(_.toString)
      parsed must equal(NonEmptySeq(expected))
    }

    it("complex") {
      val query = "select :id, balance, @b.name_last join (select * from @aaaa-aaaa as a |> select @a.name_last) as b on name_last = @b.name_last |> select :id"
      val expected1 = "SELECT `:id`, `balance`, @b.`name_last` JOIN (SELECT * FROM @aaaa-aaaa AS @a |> SELECT @a.`name_last`) AS @b ON `name_last` = @b.`name_last`"
      val expected2 = "SELECT `:id`"
      val parsed = parser.selectStatement(query).map(_.toString)
      parsed must equal(NonEmptySeq(expected1, List(expected2)))
    }
  }

  describe("query operators") {
    it("chains, unions, joins round trip") {
      val soqls = Seq(
        (
          "SELECT 1 |> SELECT 2 |> SELECT 3",
          "SELECT 1 |> SELECT 2 |> SELECT 3"
        ),
        (
          "SELECT 1 |> SELECT 2 UNION (SELECT 3 |> SELECT 4 |> SELECT 5 |> SELECT 6)",
          "SELECT 1 |> SELECT 2 UNION (SELECT 3 |> SELECT 4 |> SELECT 5 |> SELECT 6)"
        ),
        (
          "SELECT 1 UNION (SELECT 2 UNION ALL (SELECT 3 UNION SELECT 4) UNION SELECT 5) UNION SELECT 6",
          "SELECT 1 UNION (SELECT 2 UNION ALL (SELECT 3 UNION SELECT 4) UNION SELECT 5) UNION SELECT 6"
        ),
        (
          "SELECT 1 INTERSECT ALL SELECT 2",
          "SELECT 1 INTERSECT ALL SELECT 2"
        ),
        (
          "SELECT `x`, @a.`a1`, @jb.`b1`, @jcd.`c1` JOIN @a ON TRUE JOIN (SELECT @b.`b1` FROM @b) AS @jb ON TRUE JOIN (SELECT @cc.`c1` FROM @c AS @cc UNION SELECT `d1` FROM @d) AS @jcd ON TRUE |> SELECT `x`, `c1`, 1 + 2 ORDER BY `x` ASC NULL LAST, `c1` ASC NULL LAST",
          "SELECT `x`, @a.`a1`, @jb.`b1`, @jcd.`c1` JOIN @a ON TRUE JOIN (SELECT @b.`b1` FROM @b) AS @jb ON TRUE JOIN (SELECT @cc.`c1` FROM @c AS @cc UNION SELECT `d1` FROM @d) AS @jcd ON TRUE |> SELECT `x`, `c1`, 1 + 2 ORDER BY `x` ASC NULL LAST, `c1` ASC NULL LAST"
        )
      )

      soqls.foreach { case (soql, expected) =>
        val roundTrip = parser.binaryTreeSelect(soql)
        Select.toString(roundTrip) must equal(expected)
      }
    }
  }
}
