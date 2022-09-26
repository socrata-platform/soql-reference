package com.socrata.soql.analyzer2

import scala.util.parsing.input.NoPosition

import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.soql.collection._

import mocktablefinder._

class SoQLAnalysisTest extends FunSuite with MustMatchers with TestHelper {
  val rowNumber = TestFunctions.RowNumber.monomorphic.get
  val windowFunction = TestFunctions.WindowFunction.monomorphic.get
  val and = TestFunctions.And.monomorphic.get

  test("direct query does not add a column") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by text")
    analysis.preserveOrdering(rowNumber).statement.schema.size must equal (2)
  }

  test("unordered-on-unordered doesn't change under order preservation") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * |> select *")
    analysis.preserveOrdering(rowNumber).statement must equal (analysis.statement)
  }

  test("unordered-on-ordered pipes generates an ordering column") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num |> select *")
    val expectedAnalysis = analyze(tf, "twocol", "select *, row_number() over () as rn order by num |> select * (except rn) order by rn")
    analysis.preserveOrdering(rowNumber).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("ordered-on-ordered pipes generates an ordering column") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num |> select * order by text")
    val expectedAnalysis = analyze(tf, "twocol", "select *, row_number() over () as rn order by num |> select * (except rn) order by text, rn")
    analysis.preserveOrdering(rowNumber).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("orderings are blocked by aggregations and do not continue beyond them if not required by a window function") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select * order by num
  |> select *
  |> select *
  |> select text, num group by text, num order by num, text
  |> select *
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select * order by num
  |> select *
  |> select *
  |> select text, num, row_number () over () as rn group by text, num order by num, text
  |> select * (except rn) order by rn
""")

    analysis.preserveOrdering(rowNumber).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("orderings are blocked by aggregations but continue again beyond them if required") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select * order by num
  |> select *
  |> select *, window_function() over ()
  |> select text, num group by text, num order by num, text
  |> select *
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select *, row_number() over () as rn order by num
  |> select * order by rn
  |> select * (except rn), window_function() over () order by rn
  |> select text, num, row_number() over () as rn group by text, num order by num, text
  |> select * (except rn) order by rn
""")

    analysis.preserveOrdering(rowNumber).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("simple merge") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select * order by num limit 20 offset 10 |> select text + text as t2, num * 2 as num offset 10
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select text + text, num * 2 as num from @this as t order by @t.num limit 10 offset 20
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - aggregate on non-aggregate") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select text, num where num = 3 order by num |> select text, count(*) group by text
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select text, count(*) where num = 3 group by text
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - non-aggregate on aggregate") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select text, count(num) group by text |> select * where count_num = 5
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select text, count(num) as n group by text having n = 5
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - simple on windowed") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select text, row_number() over () as rn |> select text, rn + 1 limit 5
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select text, row_number() over () + 1 limit 5
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - filter on join") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "locowt") -> D("amount" -> TestNumber, "words" -> TestText)
    )

    val analysis = analyze(tf, "twocol", """
select text, num, @ct.amount, @ct.words join @locowt as @ct on num = @ct.amount |> select * where amount = 3
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select text, num, @ct.amount, @ct.words join @locowt as @ct on num = @ct.amount where @ct.amount = 3
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - join on simple") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "locowt") -> D("amount" -> TestNumber, "words" -> TestText)
    )

    val analysis = analyze(tf, "twocol", """
select text, num order by num |> select * join @locowt as ct on num = @ct.amount
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select text, num join @locowt as ct on num = @ct.amount order by num
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - implicit group-by") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select count(*), 1 as x |> select x
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select count(*), 1 as x |> select x
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused columns - simple") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select * order by num |> select text
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select text order by num |> select text
""")

    analysis.removeUnusedColumns.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused columns - preserve implicit group") {
    val tf = MockTableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select count(*), 1 as x |> select x
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select count(*), 1 as x |> select x
""")

    analysis.removeUnusedColumns.statement must be (isomorphicTo(expectedAnalysis.statement))
  }
}
