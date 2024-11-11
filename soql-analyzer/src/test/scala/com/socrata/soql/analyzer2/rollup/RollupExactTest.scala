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

  test("rollup - compatible where with clause unrelated to the rollup") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber, "flag" -> TestBoolean),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol where flag and text2 = 'world' group by text1", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol where flag group by text1, text2")
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

  test("same group by - compatible where with clause unrelated to the rollup") {
    val tf = tableFinder(
      (0, "threecol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber, "flag" -> TestBoolean),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, sum(num) from @threecol where flag and text1 > 'hello' group by text1, text2", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo(1, "rollup", tf, "select text1, text2, sum(num) from @threecol where flag group by text1, text2")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, c3 from @rollup where c1 > 'hello'", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("ad-hoc rewrite") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber)
    )

    // Because we're only inspecting the bottom 3 bits of `num`, and
    // because there's an ad-hoc transform that knows the relationship
    // between the semantics of it and those of bottom_byte (which
    // extracts the bottom 8 bits of its argument), we can use bitand
    // on the output of the bottom_dword call of rollup when we
    // rewrite.

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, bottom_byte(num) from @twocol")

    locally {
      val Right(foundTables) = tf.findTables(0, "select text, bitand(num, 7) from @twocol", Map.empty)
      val Right(analysis) = analyzer(foundTables, UserParameters.empty)
      val select = analysis.statement.asInstanceOf[Select]

      val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

      val Right(expectedRollupFT) = tf.findTables(1, "select c1, bitand(c2, 7) from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }

    // ...but we _can't_ do this if we're inspecting more than the bottom 8 bits

    locally {
      val Right(foundTables) = tf.findTables(0, "select text, bitand(num, 511) from @twocol", Map.empty)
      val Right(analysis) = analyzer(foundTables, UserParameters.empty)
      val select = analysis.statement.asInstanceOf[Select]

      TestRollupExact(select, rollup, analysis.labelProvider) must be (None)
    }
  }

  test("A parameterless aggregate cannot be used with a rollup that has the same grouping which does not produce that aggregate as output") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
    )

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, sum(num) from @twocol group by text")

    val Right(foundTables) = tf.findTables(0, "select text, count(*) from @twocol group by text", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    // So the bug was that was incorrectly saying "I _can_ use this
    // rollup to answer this query (and generating the nonsense
    // soql-equivalent "select text, count(*) from @rollup").  This
    // was happening because the rollup has the same group-by as the
    // query, so it was going into the "one to one" subpath of the
    // rewriter, and then it was saying "I can rewrite this aggregate
    // if I can rewrite all of its arguments" - but since count(*) has
    // no arguments that was incorrectly succeeding.  The more
    // accurate test is "I can do this if I'm attempting to use a
    // rollup that is not itself the result of an aggregation".
    TestRollupExact(select, rollup, analysis.labelProvider) must be (None)
  }

  // This falls out of the way we do WHERE rewriting, but adding a
  // test just to make sure it never gets accidentally broken.
  test("Exact-equality filtering can be used to pick rows out of a group-by") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber).withPrimaryKey("c1")
    )

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, sum(num) from @twocol group by text")

    val Right(foundTables) = tf.findTables(0, "select sum(num) from @twocol where text = 'hello'", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select sum(c2) from @rollup where c1 = 'hello'", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("Non-semilattice aggregates don't use raw output columns") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, num from @twocol group by text, num")

    val Right(foundTables) = tf.findTables(0, "select sum(num) from @twocol", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    TestRollupExact(select, rollup, analysis.labelProvider) must be (None)
  }

  test("Semilattice aggregates do use raw output columns") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val rollup = TestRollupInfo(1, "rollup", tf, "select text, num from @twocol group by text, num")

    val Right(foundTables) = tf.findTables(0, "select max(num) from @twocol", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select max(c2) from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("You can group by a function of grouping columns and still use the rollup") {
    val tf = tableFinder(
      (0, "names") -> D("first_name" -> TestText, "last_name" -> TestText, "count" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val rollup = TestRollupInfo(1, "rollup", tf, "select first_name, last_name, sum(count) from @names group by first_name, last_name")

    val Right(foundTables) = tf.findTables(0, "select string_func(first_name + ' ' + last_name) as name, sum(count) from @names group by name", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select string_func(c1 + ' ' + c2) as n, sum(c3) from @rollup group by n", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }
}
