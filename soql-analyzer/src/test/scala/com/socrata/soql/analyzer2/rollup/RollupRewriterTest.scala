package com.socrata.soql.analyzer2.rollup

import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.ResourceName

class RollupRewriterTest extends FunSuite with MustMatchers with RollupTestHelper with StatementUniverse[TestHelper.TestMT] {
  test("can produce multiple candidates if there are multiple rollups") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "english") -> D("num" -> TestNumber, "name" -> TestText),
      (0, "counts_by_name") -> Q(0, "twocol", "select text, @english.name, count(*) join @english on num = @english.num group by text, @english.name"),

      // These aren't actually real tables (which is why they're in a
      // different scope) but we need them to exist so we can
      // explicitly write soql that refers to them.
      (1, "rollup1") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber, "c4" -> TestText),
      (1, "rollup2") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestText, "c4" -> TestNumber).withPrimaryKey("c1","c2","c3")
    )

    val Right(foundTables) = tf.findTables(0, ResourceName("counts_by_name"))
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)

    val expr = new TestRollupRewriter(
      analysis.labelProvider,
      Seq(
        TestRollupInfo(1, "rollup1", tf, "select @twocol.text as twocol_text, @twocol.num as twocol_num, @english.num as english_num, @english.name as english_name from @twocol join @english on @twocol.num = @english.num"),
        TestRollupInfo(2, "rollup2", tf, "select text, num, @english.name, count(*) from @twocol join @english on num = @english.num group by text, num, @english.name")
      )
    )

    val result = expr.rollup(analysis.statement)
    result.length must be (2)

    locally {
      val Right(expectedRollup1FT) = tf.findTables(1, "select c1, c4, count(*) from @rollup1 group by c1, c4", Map.empty)
      val Right(expectedRollup1Analysis) = analyzer(expectedRollup1FT, UserParameters.empty)
      result(0)._1 must be (isomorphicTo(expectedRollup1Analysis.statement))
      result(0)._2 must be (Set(1))
    }

    locally {
      val Right(expectedRollup2FT) = tf.findTables(1, "select c1, c3, sum(c4) from @rollup2 group by c1, c3", Map.empty)
      val Right(expectedRollup2Analysis) = analyzer(expectedRollup2FT, UserParameters.empty)
      result(1)._1 must be (isomorphicTo(expectedRollup2Analysis.statement))
      result(1)._2 must be (Set(2))
    }
  }

  test("Will report multiple rollup-ids if multiple rollups were used") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "english") -> D("num" -> TestNumber, "name" -> TestText),

      // These aren't actually real tables (which is why they're in a
      // different scope) but we need them to exist so we can
      // explicitly write soql that refers to them.
      (1, "rollup1") -> D("c1" -> TestText, "c2" -> TestNumber),
      (1, "rollup2") -> D("c1" -> TestNumber, "c2" -> TestText)
    )

    val Right(foundTables) = tf.findTables(0, "(select text, num from @twocol) union (select name, num from @english)", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)

    val expr = new TestRollupRewriter(
      analysis.labelProvider,
      Seq(
        TestRollupInfo(1, "rollup1", tf, "select text, num from @twocol"),
        TestRollupInfo(2, "rollup2", tf, "select num, name from @english")
      )
    )

    val result = expr.rollup(analysis.statement)
    result.length must be (3) // Three because the choices are "use rollup on left, use rollup on right, use rollup on both"
    val interesting = result.filter(_._2.size == 2)
    interesting.length must be (1)

    val (stmt, rollupIds) = interesting.head
    rollupIds must be (Set(1, 2))

    val Right(expectedRollupFT) = tf.findTables(1, "(select c1, c2 from @rollup1) union (select c2, c1 from @rollup2)", Map.empty)
    val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
    stmt must be (isomorphicTo(expectedRollupAnalysis.statement))
  }
}
