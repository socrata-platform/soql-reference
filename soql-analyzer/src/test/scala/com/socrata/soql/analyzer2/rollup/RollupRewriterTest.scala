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

  test("Repeated query") {
    val tf = tableFinder(
      (0, "base") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "d1") -> Q(0, "base", "select text, sum(num) as s group by text"),
      (0, "d2") -> Q(0, "base", "select text, sum(num) as s group by text"),

      (0, "rollup1") -> D("c1" -> TestText, "c2" -> TestNumber).withPrimaryKey("c1")
    )

    val Right(foundTables) = tf.findTables(0, ResourceName("d1"), "select text, @d2.s join @d2 on text = @d2.text", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)

    val expr = new TestRollupRewriter(
      analysis.labelProvider,
      Seq(
        TestRollupInfo(0, "rollup1", tf, "select text, sum(num) from @base group by text")
      )
    )

    val rewritten = expr.rollup(analysis.statement)

    // There are six candidates:
    //    use rollup on the left in a subselect
    //    use rollup on the right in a subselect
    //    use rollup on both in a subselect
    //    use rollup on the left without a subselecct
    //    use rollup on the right without a subselect
    //    use rollup on both without a subselect
    // Annoyingly none of them can be directly implemented in soql,
    // we'll have to RemoveTrivialJoins the @single_row references to
    // get the actual isomorphism.
    val candidates = Seq(
      """select @q.text, @q.s from
        |  @single_row
        |  join (
        |    select @left.text as text, @right.c1 as text2, @right.c2 as s
        |    from
        |      @single_row
        |      join (select text, sum(num) as s from @base group by text) as @left on true
        |      join (select c1, c2 from @rollup1) as @right on @left.text = @right.c1
        |  ) as @q on true""",

      """select @q.text, @q.s from
        |  @single_row
        |  join (
        |    select @left.c1 as text, @right.text as text2, @right.s as s
        |    from
        |      @single_row
        |      join (select c1, c2 from @rollup1) as @left on true
        |      join (select text, sum(num) as s from @base group by text) as @right on @left.c1 = @right.text
        |  ) as @q on true""",

      """select @q.text, @q.s from
        |  @single_row
        |  join (
        |    select @left.c1 as text, @right.c1 as text2, @right.c2 as s
        |    from
        |      @single_row
        |      join (select c1, c2 from @rollup1) as @left on true
        |      join (select c1, c2 from @rollup1) as @right on @left.c1 = @right.c1
        |  ) as @q on true""",

      """select @left.text, @right.c2 from
        |  @single_row
        |  join (select text, sum(num) as s from @base group by text) as @left on true
        |  join (select c1, c2 from @rollup1) as @right on @left.text = @right.c1""",

      """select @left.c1, @right.s from
        |  @single_row
        |  join (select c1, c2 from @rollup1) as @left on true
        |  join (select text, sum(num) as s from @base group by text) as @right on @left.c1 = @right.text""",

      """select @left.c1, @right.c2 from
        |  @single_row
        |  join (select c1, c2 from @rollup1) as @left on true
        |  join (select c1, c2 from @rollup1) as @right on @left.c1 = @right.c1"""
    ).map(_.stripMargin)

    rewritten.size must be (candidates.size)

    for(((stmt, rollups), candidate) <- rewritten.zip(candidates)) {
      rollups must equal (Set(0))
      val Right(expectedRollupFT) = tf.findTables(0, candidate, Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
        .map(_.removeTrivialJoins { e =>
           e match {
             case LiteralValue(TestBoolean(true)) => true
             case _ => false
           }
        })
      stmt must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }
}
