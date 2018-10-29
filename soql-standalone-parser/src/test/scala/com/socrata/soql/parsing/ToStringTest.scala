package com.socrata.soql.parsing

import com.socrata.NonEmptySeq
import org.scalatest.{FunSpec, MustMatchers}

class ToStringTest extends FunSpec with MustMatchers {
  val parser = new StandaloneParser()

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
      val expected1 = "SELECT `:id`, `balance` AS amt, `visits`"
      val expected2 = "SELECT `:id` AS i, sum(`amt`) WHERE `visits` > 0 GROUP BY `i`, `visits` HAVING `sum_amt` < 5 ORDER BY `i` DESC NULL FIRST, sum(`amt`) ASC NULL FIRST LIMIT 5 OFFSET 10 SEARCH 'gnu'"
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
      val expected = "SELECT `:id`, @a.`name_last` JOIN @aaaa-aaaa AS a ON `name_last` = @a.`name_last`"
      val parsed = parser.selectStatement(query).map(_.toString)
      parsed must equal(NonEmptySeq(expected))
    }

    it("complex") {
      val query = "select :id, balance, @b.name_last join (select * from @aaaa-aaaa as a |> select @a.name_last) as b on name_last = @b.name_last |> select :id"
      val expected1 = "SELECT `:id`, `balance`, @b.`name_last` JOIN (SELECT * FROM @aaaa-aaaa AS a |> SELECT @a.`name_last`) AS b ON `name_last` = @b.`name_last`"
      val expected2 = "SELECT `:id`"
      val parsed = parser.selectStatement(query).map(_.toString)
      parsed must equal(NonEmptySeq(expected1, List(expected2)))
    }
  }
}
