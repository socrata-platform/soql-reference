package com.socrata.soql.analyzer2

import scala.util.parsing.input.NoPosition

import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.soql.collection._

import mocktablefinder._

class SoQLAnalysisTest extends FunSuite with MustMatchers with TestHelper {
  val analyzer = new SoQLAnalyzer[Int, TestType, TestValue](TestTypeInfo, TestFunctionInfo)
  val rowNumber = TestFunctions.RowNumber.monomorphic.get
  val windowFunction = TestFunctions.WindowFunction.monomorphic.get
  val and = TestFunctions.And.monomorphic.get

  test("direct query does not add a column") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val tf.Success(start) = tf.findTables(0, rn("twocol"), "select * order by text")
    val analysis = analyzer(start, UserParameters.empty)

    analysis.preserveOrdering(rowNumber).statement.schema.size must equal (2)
  }

  test("unordered-on-unordered doesn't change under order preservation") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val tf.Success(start) = tf.findTables(0, rn("twocol"), "select * |> select *")
    val analysis = analyzer(start, UserParameters.empty)

    analysis.preserveOrdering(rowNumber).statement must equal (analysis.statement)
  }

  test("unordered-on-ordered pipes generates an ordering column") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val tf.Success(start) = tf.findTables(0, rn("twocol"), "select * order by num |> select *")
    val analysis = analyzer(start, UserParameters.empty)

    val tf.Success(start2) = tf.findTables(0, rn("twocol"), "select *, row_number() over () as rn order by num |> select * (except rn) order by rn")
    val expectedAnalysis = analyzer(start2, UserParameters.empty)

    analysis.preserveOrdering(rowNumber).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("ordered-on-ordered pipes generates an ordering column") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val tf.Success(start) = tf.findTables(0, rn("twocol"), "select * order by num |> select * order by text")
    val analysis = analyzer(start, UserParameters.empty)

    val tf.Success(start2) = tf.findTables(0, rn("twocol"), "select *, row_number() over () as rn order by num |> select * (except rn) order by text, rn")
    val expectedAnalysis = analyzer(start2, UserParameters.empty)

    analysis.preserveOrdering(rowNumber).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("orderings are blocked by aggregations and do not continue beyond them if not required by a window function") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val tf.Success(start) = tf.findTables(0, rn("twocol"), """
select * order by num
  |> select *
  |> select *
  |> select text, num group by text, num order by num, text
  |> select *
""")
    val analysis = analyzer(start, UserParameters.empty)

    val tf.Success(start2) = tf.findTables(0, rn("twocol"), """
select * order by num
  |> select *
  |> select *
  |> select text, num, row_number () over () as rn group by text, num order by num, text
  |> select * (except rn) order by rn
""")
    def expectedAnalysis = analyzer(start2, UserParameters.empty)

    analysis.preserveOrdering(rowNumber).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("orderings are blocked by aggregations but continue again beyond them if required") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val tf.Success(start) = tf.findTables(0, rn("twocol"), """
select * order by num
  |> select *
  |> select *, window_function() over ()
  |> select text, num group by text, num order by num, text
  |> select *
""")
    val analysis = analyzer(start, UserParameters.empty)

    val tf.Success(start2) = tf.findTables(0, rn("twocol"), """
select *, row_number() over () as rn order by num
  |> select * order by rn
  |> select * (except rn), window_function() over () order by rn
  |> select text, num, row_number() over () as rn group by text, num order by num, text
  |> select * (except rn) order by rn
""")
    val expectedAnalysis = analyzer(start2, UserParameters.empty)

    analysis.preserveOrdering(rowNumber).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("simple merge") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val tf.Success(start) = tf.findTables(0, rn("twocol"), """
select * order by num limit 20 offset 10 |> select text + text as t2, num * 2 as num offset 10
""")
    val analysis = analyzer(start, UserParameters.empty)

    val tf.Success(start2) = tf.findTables(0, rn("twocol"), """
select text + text, num * 2 as num from @this as t order by @t.num limit 10 offset 20
""")
    val expectedAnalysis = analyzer(start2, UserParameters.empty)

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }
}
