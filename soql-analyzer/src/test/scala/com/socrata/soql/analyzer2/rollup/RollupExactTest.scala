package com.socrata.soql.analyzer2.rollup

import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._

class RollupExactTest extends FunSuite with MustMatchers with RollupTestHelper with StatementUniverse[TestHelper.TestMT] {
  test("simple exact") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select text, num from @twocol where num = 5", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, num * 5, num from @twocol where num = 5 order by text")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, c3 from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("simple subset exact") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select text, bottom_byte(num) from @twocol where num = 5", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, num * 5, bottom_dword(num) from @twocol where num = 5 order by text")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, bottom_byte(c3) from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("simple subset-from") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select text, num from @twocol |> select text, bottom_byte(num) where num = 5", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, num, num+num from @twocol |> select text, num * 5, bottom_dword(num) where num = 5 order by text")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, bottom_byte(c3) from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("simple actual rollup") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "another_num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text, sum(another_num) from @threecol group by text", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, num, sum(another_num) from @threecol group by text, num")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, sum(c3) from @rollup group by c1", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("function subset rollup") {
    val tf = tableFinder(
      (0, "twocol") -> D("num1" -> TestNumber, "num2" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestNumber, "c2" -> TestNumber).withPrimaryKey("c1")
    )

    val Right(foundTables) = tf.findTables(0, "select bottom_byte(num1), max(num2) from @twocol group by bottom_byte(num1)", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select bottom_dword(num1), max(num2) from @twocol group by bottom_dword(num1)")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select bottom_byte(c1), max(c2) from @rollup group by bottom_byte(c1)", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("simple rollup with additional AND in where") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select * from @twocol where num > 5 and num < 10", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select * from @twocol where num > 5")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, c2 from @rollup where c2 < 10", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("rollup avg - identical group by") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber).withPrimaryKey("c1")
    )

    val Right(foundTables) = tf.findTables(0, "select text, avg(num) from @twocol group by text", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, sum(num), count(num) from @twocol group by text")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, c2/c3 from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("rollup avg - coarser group by") {
    val tf = tableFinder(
      (0, "twocol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber, "c4" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, avg(num) from @twocol group by text1", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num), count(num) from @twocol group by text1, text2")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, sum(c3)/sum(c4) from @rollup group by c1", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("rollup - incompatible where") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol where num = 5 group by text1", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol group by text1, text2")
    TestRollupExact(select, rollup, analysis.labelProvider) must be (None)
  }

  test("rollup - compatible where") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol where text2 = 'world' group by text1", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol group by text1, text2")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, sum(c3) from @rollup where c2 = 'world' group by c1", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("rollup - refining where") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol where text1 = 'hello' and text2 = 'world' group by text1", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol where text2 = 'world' group by text1, text2")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, sum(c3) from @rollup where c1 = 'hello' group by c1", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("rollup - unrefinable where") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol group by text1", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol where text1 = 'hello' group by text1, text2")
    TestRollupExact(select, rollup, analysis.labelProvider) must be (None)
  }

  test("same group by - incompatible where") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol where num = 5 group by text1, text2", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol group by text1, text2")
    TestRollupExact(select, rollup, analysis.labelProvider) must be (None)
  }

  test("same group by - compatible where") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol where text2 = 'world' group by text1, text2", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol group by text1, text2")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, c3 from @rollup where c2 = 'world'", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("same group by - refining where") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol where text1 = 'hello' and text2 = 'world' group by text1, text2", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol where text2 = 'world' group by text1, text2")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, c3 from @rollup where c1 = 'hello'", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("same group by - unrefinable where") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol group by text1, text2", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol where text1 = 'hello' group by text1, text2")
    TestRollupExact(select, rollup, analysis.labelProvider) must be (None)
  }
}
