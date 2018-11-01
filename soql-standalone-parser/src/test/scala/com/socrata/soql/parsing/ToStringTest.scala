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

  describe("TableName") {
    import com.socrata.soql.environment.TableName._
    import Prefixers.FourBy4._

    val validExamples = List("aaaa-aaaa", "22pn-yhhs", "g34u-2aa5", "6z52-mkqf", "29ck-sy8b", "6syt-4pv9", "em7z-qeb7", "gqa6-8x5q", "7ndm-ubkq", "7k65-66d2", "dt82-jwgj", "8qy9-my8i", "wpc5-2rbf", "xxik-zkrp", "jhpt-dwhi", "8vca-8m4p", "qt42-nunc", "iphu-cs44", "xpwe-dz2q", "ge78-zdn5", "7ecp-3dim", "w5u5-s6wy", "gg2y-6avg", "x6mi-xdzd", "fqjb-ys7k", "j878-by6b", "gxh2-5hm6", "vu75-ih58", "rrgq-keyp", "imuv-ja3w", "thw2-8btq", "qc2h-htfs", "ky7w-js83", "u9tw-zxuv", "g8qq-u3cb", "dgsm-fppf", "q4i3-ds2x", "e8gb-ruhc", "8yms-nqbp", "fb43-rvc3", "ykke-a4sz", "x5hp-ga5h", "w24e-aezw", "m933-w2uc", "bu4p-snct")
    val invalidExamples = List("aaaa-aaa1", "l234-abcd", "aaa-aaaa", "aaaa-aaaaa", "aaaaaa-aaaa", "aaaaaaaa", "0aaa-aaaa", "aaoa-aaaa", "select * from aaaa-aaaa")

    val commonPrefixes = List("@", "_", "t")
    val uncommonPrefixes = List(":", "6", "z", ".")

    val prefixedValidExamples = (commonPrefixes ++ uncommonPrefixes).flatMap(p => validExamples.map(e => s"$p$e"))

    it("removing prefix for unprefixed valid 4x4s causes no change") {
      validExamples.foreach { valid =>
        removePrefix(valid) must equal(valid)
      }
    }

    it("removing prefix for non-4x4s causes no change") {
      invalidExamples.foreach { valid =>
        removePrefix(valid) must equal(valid)
      }
    }

    it("removing prefix from prefixed valid 4x4 removes any single character prefix") {
      prefixedValidExamples.foreach { prefixed =>
        removePrefix(prefixed) must equal(prefixed.substring(1))
      }
    }

    it("withSoqlPrefix adds @ for unprefixed valid 4x4s") {
      validExamples.foreach { valid =>
        withSoqlPrefix(valid) must equal(s"$SoqlPrefix$valid")
      }
    }

    it("withSoqlPrefix maintains @ for prefixed @4x4s") {
      validExamples.foreach { valid =>
        withSoqlPrefix(s"$SoqlPrefix$valid") must equal(s"$SoqlPrefix$valid")
      }
    }

    it("withSoqlPrefix replaces _ for prefixed _4x4s") {
      validExamples.foreach { valid =>
        withSoqlPrefix(s"$SodaFountainPrefix$valid") must equal(s"$SoqlPrefix$valid")
      }
    }

    it("withSoqlPrefix does not add @ for non-4x4s") {
      invalidExamples.foreach { invalid =>
        withSoqlPrefix(invalid) must equal(invalid)
      }
    }

    it("withSodaFountainPrefix adds _ for unprefixed valid 4x4s") {
      validExamples.foreach { valid =>
        withSodaFountainPrefix(valid) must equal(s"$SodaFountainPrefix$valid")
      }
    }

    it("withSodaFountainPrefix maintains _ for prefixed valid 4x4s") {
      validExamples.foreach { valid =>
        withSodaFountainPrefix(s"$SodaFountainPrefix$valid") must equal(s"$SodaFountainPrefix$valid")
      }
    }

    it("withSodaFountainPrefix replaces @ for prefixed valid @4x4s") {
      validExamples.foreach { valid =>
        withSodaFountainPrefix(s"$SoqlPrefix$valid") must equal(s"$SodaFountainPrefix$valid")
      }
    }

    it("withSodaFountainPrefix does not add _ for non-4x4s") {
      invalidExamples.foreach { invalid =>
        withSodaFountainPrefix(invalid) must equal(invalid)
      }
    }

  }
}
