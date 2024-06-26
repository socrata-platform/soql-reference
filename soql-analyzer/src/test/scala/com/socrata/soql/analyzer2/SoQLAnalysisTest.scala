package com.socrata.soql.analyzer2

import scala.util.parsing.input.NoPosition

import org.scalatest.{FunSuite, MustMatchers}
import com.rojoma.json.v3.interpolation._

import com.socrata.soql.collection._
import com.socrata.soql.serialize.{ReadBuffer, WriteBuffer}

import mocktablefinder._

class SoQLAnalysisTest extends FunSuite with MustMatchers with TestHelper {
  val rowNumber = TestFunctions.RowNumber.monomorphic.get
  val windowFunction = TestFunctions.WindowFunction.monomorphic.get
  val and = TestFunctions.And.monomorphic.get

  test("preserve ordering - direct query does not add a column") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by text")
    analysis.preserveOrdering.statement.schema.size must equal (2)
  }

  test("preserve ordering - unordered-on-unordered doesn't change under order preservation") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * |> select *")
    analysis.preserveOrdering.statement must equal (analysis.statement)
  }

  test("preserve ordering - unordered-on-ordered pipes does not generate an ordering column if unnecessary") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num |> select *")
    val expectedAnalysis = analyze(tf, "twocol", "select * order by num |> select * order by num")
    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering - unordered-on-ordered pipes generates a synthetic ordering column if necessary") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num+1 |> select *").preserveOrdering
    val expectedAnalysis = analyze(tf, "twocol", "select *, num+1 as ordering order by num+1 |> select * (except ordering) order by ordering")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))

    analysis.statement match {
      case s: Select[TestMT] =>
        s.from.schema.last.isSynthetic must be (true)
      case _ =>
        fail("Somehow not from a select??")
    }
  }

  test("preserve ordering - ordered-on-ordered pipes does not generate an ordering column if unnecessary") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num |> select * order by text")
    val expectedAnalysis = analyze(tf, "twocol", "select * order by num |> select * order by text, num")
    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering - ordered-on-ordered pipes generates an ordering column") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num+1 |> select * order by text")
    val expectedAnalysis = analyze(tf, "twocol", "select *, num+1 as ordering order by num+1 |> select * (except ordering) order by text, ordering")
    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering - orderings are blocked by aggregations and do not continue beyond them") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select * order by num+1
  |> select *
  |> select *
  |> select text, num group by text, num order by num+2, text+'x'
  |> select *
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select * order by num+1
  |> select *
  |> select *
  |> select text, num, num+2 as o1, text+'x' as o2 group by text, num order by num+2, text+'x'
  |> select * (except o1, o2) order by o1, o2
""")

    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering - output columns are not added") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """select * order by num + 1 |> select * order by string_func(text)""")
    val expectedAnalysis = analyze(tf, "twocol", """select *, num+1 order by num + 1 |> select text, num order by string_func(text), num_1""")
    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering with output columns - output columns are added") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """select * order by num + 1 |> select * order by string_func(text)""")
    val expectedAnalysis = analyze(tf, "twocol", """select *, num+1 order by num + 1 |> select text, num, string_func(text), num_1 order by string_func(text), num_1""")
    analysis.dangerous.preserveOrderingWithColumns.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering with output columns - output columns are added without inheritance") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """select * order by num + 1""")
    val expectedAnalysis = analyze(tf, "twocol", """select *, num+1 order by num + 1""")
    analysis.dangerous.preserveOrderingWithColumns.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering with output columns - already-selected output columns are not re-added") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """select text, num*2 order by num*2, string_func(text)""")
    val expectedAnalysis = analyze(tf, "twocol", """select text, num*2, string_func(text) order by num*2, string_func(text)""")
    analysis.dangerous.preserveOrderingWithColumns.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("simple merge") {
    val tf = tableFinder(
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

  test("merge - distinct on ordered by different column") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select text, num order by num |> select distinct text
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select distinct text
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - distinct on ordered by same column") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select text, num order by text |> select distinct text
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select distinct text order by text
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - unsimple distinct on ordered") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select text, num order by num |> select distinct text, num order by text
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select distinct text, num order by text, num
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - distinct aggregate on ordered") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select text, num order by num |> select distinct text group by text
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select distinct text group by text
""")

    analysis.merge(and).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("merge - aggregate on non-aggregate") {
    val tf = tableFinder(
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
    val tf = tableFinder(
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
    val tf = tableFinder(
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
    val tf = tableFinder(
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
    val tf = tableFinder(
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
    val tf = tableFinder(
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

  test("merge - preserve inherited hints - hint on base") {
    val tf = tableFinder(
      (0, "ds") -> D("a" -> TestText).withOutputColumnHints("a" -> j"true")
    )

    val analysis = analyze(tf, "ds", """
select a |> select a |> select a
""").merge(and)

    val expectedAnalysis = analyzeSaved(tf, "ds")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
    analysis.statement.schema.values.map(_.hint).toSeq must be (Seq(Some(j"true")))
  }

  test("merge - preserve inherited hints - hint on intermediate") {
    val tf = tableFinder(
      (0, "ds") -> D("a" -> TestText),
      (0, "q") -> Q(0, "ds", "select a").withOutputColumnHints("a" -> j"true")
    )

    val analysis = analyze(tf, "q", """
select a |> select a |> select a
""").merge(and)

    val expectedAnalysis = analyzeSaved(tf, "ds")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
    analysis.statement.schema.values.map(_.hint).toSeq must be (Seq(Some(j"true")))
  }

  test("merge - preserve inherited hints - hint on last") {
    val tf = tableFinder(
      (0, "ds") -> D("a" -> TestText),
      (0, "q1") -> Q(0, "ds", "select a"),
      (0, "q2") -> Q(0, "q1", "select a").withOutputColumnHints("a" -> j"true")
    )

    val analysis = analyzeSaved(tf, "q2").merge(and)

    val expectedAnalysis = analyzeSaved(tf, "ds")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
    analysis.statement.schema.values.map(_.hint).toSeq must be (Seq(Some(j"true")))
  }

  test("merge - lateral join") {
    val tf = tableFinder(
      (0, "ds1") -> D("a" -> TestText, "key" -> TestNumber),
      (0, "ds2") -> D("b" -> TestText, "key" -> TestNumber),
      (0, "q1") -> Q(0, "ds1", "select a, @b2.b, @b2.fk, @b2.fa from @this as t join lateral (select b, @t.key as fk from @ds2 |> select *, @t.a as fa) as b2 on true"),
      (0, "q2") -> Q(0, "ds1", "select a, @b2.b, @b2.fk, @b2.fa from @this as t join lateral (select b, @t.key as fk, @t.a as fa from @ds2) as b2 on true")
    )

    val analysis = analyzeSaved(tf, "q1").merge(and)

    val expectedAnalysis = analyzeSaved(tf, "q2")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused columns - simple") {
    val tf = tableFinder(
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
    val tf = tableFinder(
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

  test("remove unused columns - udf") {
    val tf = tableFinder(
      (0, "twocol") -> D("b" -> TestBoolean, "n" -> TestNumber, "t" -> TestText),
      (0, "udf") -> U(0, "select from @single_row where ?x", "x" -> TestBoolean)
    )

    // Force @twocol into the subselect so there's a query for columns
    // to be removed from, then pass one of its columns which is
    // otherwise not used into a UDF.
    val analysis = analyze(tf, "twocol", """
select * |> select n join @udf(b) as @udf on true
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select b, n |> select n join @udf(b) as @udf on true
""")

    analysis.removeUnusedColumns.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused order by - preserve top-level ordering") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num order by text")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num order by text")

    analysis.removeUnusedOrderBy.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused order by - remove trivially unused intermediate ordering") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    // the "+1" prevents the remove-unused-order-by from saying "hey,
    // this subselect is now completely trivial, delete it."
    val analysis = analyze(tf, "twocol", "select text, num+1 order by text |> select text, num_1 order by num_1")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num+1 |> select text, num_1 order by num_1")

    analysis.removeUnusedOrderBy.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused order by - remove subselects that become trivial") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num order by text |> select text, num order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num order by num")

    analysis.removeUnusedOrderBy.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused order by - window functions don't force ordering to be kept") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num, window_function() over () order by text |> select text, num order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num, window_function() over () |> select text, num order by num")

    analysis.removeUnusedOrderBy.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused order by - keep ordering when there's a limit involved") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num order by text limit 5 |> select text, num order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num order by text limit 5 |> select text, num order by num")

    analysis.removeUnusedOrderBy.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused order by - keep ordering when there's an offset involved") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num order by text offset 5 |> select text, num order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num order by text offset 5 |> select text, num order by num")

    analysis.removeUnusedOrderBy.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering - simple") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber).withOrdering("text", ascending = false)
    )

    val analysis = analyze(tf, "twocol", "select *")
    val expectedAnalysis = analyze(tf, "twocol", "select * order by text desc")

    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering + remove unused order by - keep ordering only at the top level") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    // the "+1" prevents the remove-unused-order-by from saying "hey,
    // this subselect is now completely trivial, delete it."
    val analysis = analyze(tf, "twocol", "select text, num+1 order by text |> select text, num_1 order by num_1")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num+1 |> select text, num_1 order by num_1, text")

    analysis.preserveOrdering.removeUnusedOrderBy.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering - non-duplicate chained") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber).withOrdering("text", ascending = false),
    )

    val analysis = analyze(tf, "twocol", "select text, num order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num order by num, text desc")

    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve ordering - duplicate chained") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber).withOrdering("text", ascending = false),
    )

    val analysis = analyze(tf, "twocol", "select text, num order by text")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num order by text")

    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - primary key") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select text, num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num order by :id")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - group by") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select text, sum(num) as sum group by text order by sum desc")
    val expectedAnalysis = analyze(tf, "twocol", "select text, sum(num) group by text order by sum(num) desc, text")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - group by + inherited PK") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber, "unorderable" -> TestUnorderable).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select unorderable, :id, sum(num) as sum group by unorderable, :id order by sum desc")
    val expectedAnalysis = analyze(tf, "twocol", "select unorderable, :id, sum(num) group by unorderable, :id order by sum(num) desc, :id")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - inherited PK is not selected") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select text, sum(num) group by text |> select sum_num").imposeOrdering(testTypeInfo.isOrdered)
    val expectedAnalysis = analyze(tf, "twocol", "select text, sum(num) group by text |> select sum_num order by text")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - nested group by") {
    val tf = tableFinder(
      (0, "twocol") -> D("id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey("id")
    )

    val analysis = analyze(tf, "twocol", "select sum(num) group by text |> select sum_num").imposeOrdering(testTypeInfo.isOrdered)
    val expectedAnalysis = analyze(tf, "twocol", "select sum(num), text group by text |> select sum_num order by text")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - partial distinct on") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select distinct on (text, num*2) text, num order by text desc")
    val expectedAnalysis = analyze(tf, "twocol", "select distinct on (text, num*2) text, num order by text desc, num*2, num")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - partial distinct on & second stage") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select distinct on (text, num*2) text, num order by num*2 desc |> select *")
    // Yes, "order by text, num" on the end there.  We didn't ask it
    // to _preserve_ an ordering, we asked it to impose an arbitrary
    // one.  Since the final select doesn't claim to care, we'll
    // happily destroy the previous ordering.
    val expectedAnalysis = analyze(tf, "twocol", "select distinct on (text, num*2) text, num order by num*2 desc |> select text, num order by text, num")

    // But, if we make the final select care by telling asking the
    // analysis to preserve ordering before imposing one, we're good.
    val expectedAnalysisWithPreservation = analyze(tf, "twocol", "select distinct on (text, num*2) text, num, num*2 as num_2 order by num_2 desc |> select text, num order by num_2 desc, text, num")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
    analysis.preserveOrdering.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysisWithPreservation.statement))
  }

  test("impose ordering - partial distinct on & second stage with PK") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select distinct on (text, num*2) text, num order by num*2 desc |> select *")
    // Again, without being asked to preserve the ordering we'll
    // destroy it when we impose one.
    val expectedAnalysis = analyze(tf, "twocol", "select distinct on (text, num*2) text, num, :id order by num*2 desc |> select text, num order by :id")

    // But if we do ask, we'll automatically plumb the primary key
    // through and use it to disambiguate the order by.
    val expectedAnalysisWithPreservation = analyze(tf, "twocol", "select distinct on (text, num*2) text, num, num*2 as num_2, :id order by num_2 desc |> select text, num order by num_2 desc, :id")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
    analysis.preserveOrdering.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysisWithPreservation.statement))
  }

  test("impose ordering - non-distinct order by") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select distinct on (text) text, num order by text, num*3 desc")
    val expectedAnalysis = analyze(tf, "twocol", "select distinct on (text) text, num order by text, num*3 desc, num")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - indistinct") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*2 order by text, num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*2 order by text, num, num*2")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - join") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id"),
      (0, "threecol") -> D(":id" -> TestNumber, "a" -> TestText, "b" -> TestText, "c" -> TestText).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select text, num*2 join @threecol on true order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*2 join @threecol on true order by num, :id, @threecol.:id")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - union without unique") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "altcol") -> D("words" -> TestText, "digits" -> TestNumber),
      // Can't put a subselect in initial-FROM position in soql
      // directly, so we need a saved query to use for that
      (0, "union") -> Q(0, "twocol", "select * union select * from @altcol")
    )

    val analysis = analyze(tf, "twocol", "select * union select * from @altcol")
    val expectedAnalysis = analyze(tf, "union", "select * order by text, num")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - minus with primary key") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber).withPrimaryKey("text"),
      (0, "altcol") -> D("words" -> TestText, "digits" -> TestNumber),
      // Can't put a subselect in initial-FROM position in soql
      // directly, so we need a saved query to use for that
      (0, "minus") -> Q(0, "twocol", "select * minus select * from @altcol")
    )

    val analysis = analyze(tf, "twocol", "select * minus select * from @altcol")
    val expectedAnalysis = analyze(tf, "minus", "select * order by text")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - unorderable") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "another_text" -> TestUnorderable)
    )

    val analysis = analyze(tf, "threecol", "select *")
    val expectedAnalysis = analyze(tf, "threecol", "select * order by text, num")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - across stages") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select text, num, num + num as num2 |> select text, num2 order by num2")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num, num+num, :id |> select text, num_num order by num_num, :id")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - join and follow-up stage") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id"),
      (0, "threecol") -> D(":id" -> TestNumber, "a" -> TestText, "b" -> TestText, "c" -> TestText).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select *, @threecol.* join @threecol on true |> select text, b order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select *, @threecol.*, :id, @threecol.:id as tcid join @threecol on true |> select text, b order by num, :id, tcid")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - multiple primary keys") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id").withPrimaryKey("text")
    )

    val analysis = analyze(tf, "twocol", "select num |> select 1")
    val expectedAnalysis = analyze(tf, "twocol", "select num, :id, text |> select 1 order by :id")

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - already selected") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id").withPrimaryKey("text")
    )

    val analysis = analyze(tf, "twocol", "select text, num |> select 1")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num, :id |> select 1 order by :id") // it'll choose :id to order by just because it comes first

    analysis.imposeOrdering(testTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - combined tables") {
    val tf = tableFinder(
      (0, "a") -> D("a1" -> TestText, "a2" -> TestNumber),
      (0, "b") -> D("b1" -> TestText, "b2" -> TestNumber)
    )

    val analysis = analyze(tf, "(select * from @a) union (select * from @b)")
    val expectedAnalysis = analyze(tf, "((select * from @a) union (select * from @b)) |> select * limit 25 offset 50")
    analysis.addLimitOffset(limit = Some(nnbi(25)), offset = Some(nnbi(50))).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - no limit/offset initially") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 25 offset 50")
    analysis.addLimitOffset(limit = Some(nnbi(25)), offset = Some(nnbi(50))).statement must be (isomorphicTo(expectedAnalysis.statement))
  }


  test("addLimitOFfset - fits within initial limit/offset") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 25 offset 150")
    analysis.addLimitOffset(limit = Some(nnbi(25)), offset = Some(nnbi(50))).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - overlaps the end of initial limit/offset") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 50 offset 150")
    analysis.addLimitOffset(limit = Some(nnbi(500)), offset = Some(nnbi(50))).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - overlaps the end of initial limit/offset with no new limit provided") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 50 offset 150")
    analysis.addLimitOffset(limit = None, offset = Some(nnbi(50))).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - passes the end of initial limit/offset") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 0 offset 200") // Note the offset will not pass the end of the original chunk
    analysis.addLimitOffset(limit = Some(nnbi(500)), offset = Some(nnbi(500))).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - passes the end of initial limit/offset with no new limit provided") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 0 offset 200") // Note the offset will not pass the end of the original chunk
    analysis.addLimitOffset(limit = None, offset = Some(nnbi(500))).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("inline parameters - all complex") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "udf") -> U(0, "select 1 from @single_row where ?name is null and ?count = 5", "name" -> TestText, "count" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * join @udf(text + 'gnu', num + 2) on true")
    val simplified = analysis.inlineTrivialParameters(isLiteralTrue)
    simplified.statement must be (isomorphicTo(analysis.statement))
  }

  test("inline parameters - one complex") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "udf") -> U(0, "select 1 from @single_row where ?name is null and ?count = 5", "name" -> TestText, "count" -> TestNumber)
    )

    val tf2 = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "udf") -> U(0, "select 1 from @single_row where 'gnu' is null and ?count = 5", "count" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * join @udf('gnu', num + 2) on true").inlineTrivialParameters(isLiteralTrue)
    val expected = analyze(tf2, "twocol", "select * join @udf(num + 2) on true")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("inline parameters - no complex") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "udf") -> U(0, "select 1 as one from @single_row where ?name is null and ?count = 5", "name" -> TestText, "count" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select *, @udf.one join @udf('gnu', num) on true").inlineTrivialParameters(isLiteralTrue)
    val expected = analyze(tf, "twocol", "select *, @udf.one join lateral (select 1 as one from @single_row where 'gnu' is null and num = 5) as @udf on true")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("inline parameters - no complex and interior union") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "udf") -> U(0, "(select 1 as one from @single_row where ?name is null and ?count = 5) union (select 2 as two from @single_row where ?name = 'haha' and ?count = 6)", "name" -> TestText, "count" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select *, @udf.one join @udf('gnu', num) on true").inlineTrivialParameters(isLiteralTrue)
    val expected = analyze(tf, "twocol", "select *, @udf.one join lateral ((select 1 as one from @single_row where 'gnu' is null and num = 5) union (select 2 as two from @single_row where 'gnu' = 'haha' and num = 6)) as @udf on true")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("remove trivial selects") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "num2" -> TestNumber)
    )
    val analysis = analyze(tf, "threecol", "select text, num |> select text").removeTrivialSelects
    val expected = analyze(tf, "threecol", "select text")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("remove trivial selects - preserve local hints") {
    val tf = tableFinder(
      (0, "ds") -> D("a" -> TestText),
      (0, "q") -> Q(0, "ds", "select a"),
      (0, "q2") -> Q(0, "q", "select a").withOutputColumnHints("a" -> j"true"),
      (0, "q3") -> Q(0, "ds", "select a").withOutputColumnHints("a" -> j"true")
    )

    val analysis = analyzeSaved(tf, "q2").removeTrivialSelects
    val expected = analyzeSaved(tf, "q3")

    analysis.statement must be (isomorphicTo(expected.statement))
    analysis.statement.schema.values.map(_.hint).toSeq must be (Seq(Some(j"true")))
  }

  test("remove trivial selects - preserve inherited hints") {
    val tf = tableFinder(
      (0, "ds") -> D("a" -> TestText),
      (0, "q1") -> Q(0, "ds", "select a").withOutputColumnHints("a" -> j"false"),
      (0, "q2") -> Q(0, "q1", "select a").withOutputColumnHints("a" -> j"true"),
      (0, "q3") -> Q(0, "q2", "select a"),
      (0, "w") -> Q(0, "ds", "select a").withOutputColumnHints("a" -> j"true")
    )

    val analysis = analyzeSaved(tf, "q3").removeTrivialSelects
    val expected = analyzeSaved(tf, "w")

    analysis.statement must be (isomorphicTo(expected.statement))
    analysis.statement.schema.values.map(_.hint).toSeq must be (Seq(Some(j"true")))
  }

  test("remove trivial selects - chained") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "num2" -> TestNumber)
    )
    val analysis = analyze(tf, "threecol", "select text, num |> select text, num |> select text, num |> select text").removeTrivialSelects
    val expected = analyze(tf, "threecol", "select text")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("remove trivial selects - after a union") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "num2" -> TestNumber),
      (0, "threemorecol") -> D("t" -> TestText, "n1" -> TestNumber, "n2" -> TestNumber)
    )
    val analysis = analyze(tf, "threecol", "(select * union select * from @threemorecol) |> select text, num |> select text").removeTrivialSelects
    val expected = analyze(tf, "threecol", "(select text, num, num2 union select t, n1, n2 from @threemorecol) |> select text")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("remove trivial selects - in a subquery, completely removable") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "num2" -> TestNumber),
      (0, "threemorecol") -> D("t" -> TestText, "n1" -> TestNumber, "n2" -> TestNumber)
    )
    val analysis = analyze(tf, "threecol", "select *, @x.text as xtext join (select t as text, n1 from @threemorecol |> select text) as @x on true").removeTrivialSelects
    val expected = analyze(tf, "threecol", "select text, num, num2, @x.t as xtext join (select t from @threemorecol) as @x on true")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("remove trivial selects - in a subquery, incompletely removable") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "num2" -> TestNumber),
      (0, "threemorecol") -> D("t" -> TestText, "n1" -> TestNumber, "n2" -> TestNumber)
    )
    val analysis = analyze(tf, "threecol", "select *, @x.t2 join (select t as text, n1 from @threemorecol |> select text+text as t2) as @x on true").removeTrivialSelects
    val expected = analyze(tf, "threecol", "select text, num, num2, @x.t2 join (select t, n1 from @threemorecol |> select t+t as t2) as @x on true")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("remove trivial selects - rewrite across lateral joins") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "num2" -> TestNumber),
      (0, "threemorecol") -> D("t" -> TestText, "n1" -> TestNumber, "n2" -> TestNumber)
    )
    val analysis = analyze(tf, "threecol", "select * |> select text, num join lateral (select t as text, n1 from @threemorecol |> select text, num2) as @x on true").removeTrivialSelects
    val expected = analyze(tf, "threecol", "select * |> select text, num join lateral (select t, num2 from @threemorecol) as @x on true")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("remove trivial selects - search with rewrite not permitted") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "num2" -> TestNumber)
    )

    // Can't rewrite here because "search" in the third stage cares
    // about the whole input schema, so the fact that `select
    // text,num` narrows it from three columns to two is important.
    val analysis = analyze(tf, "threecol", "select text, num |> select * search 'search'").removeTrivialSelects
    val expected = analyze(tf, "threecol", "select text, num |> select text, num search 'search'")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("remove trivial selects - search with rewrite not permitted, more subtly") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "num2" -> TestNumber)
    )

    // Can't rewrite here because "search" in the third stage cares
    // about the whole input schema, so the fact that `select
    // text,num2,num` reorders the input columns is important.
    val analysis = analyze(tf, "threecol", "select text, num2, num |> select * search 'search'").removeTrivialSelects
    val expected = analyze(tf, "threecol", "select text, num2, num |> select text, num2, num search 'search'")

    analysis.statement must be (isomorphicTo(expected.statement))
  }

  test("simple (de)serialization") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "names") -> D("num" -> TestNumber, "first" -> TestText, "last" -> TestText),
      (0, "join") -> Q(0, "names", "SELECT first, last, @twocol.text JOIN @twocol ON num = @twocol.num")
    )

    val analysis = analyze(tf, "join", """
select * where first = 'Tom'
""")

    implicit val mfDeser = com.socrata.soql.functions.MonomorphicFunction.deserialize(TestFunctionInfo)
    val deser = ReadBuffer.read[SoQLAnalysis[TestMT]](WriteBuffer.asBytes(analysis))
    deser.statement must equal (analysis.statement)
  }

  test("subset - simple") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val superAnalysis = analyze(tf, "twocol", "select *")
    val subAnalysis = analyze(tf, "twocol", "select text")

    subAnalysis.statement must be (verticalSliceOf(superAnalysis.statement))
  }

  test("subset - compound") {
    val tf = tableFinder(
      (0, "twocol") -> D("a" -> TestText, "b" -> TestNumber, "c" -> TestNumber)
    )

    val superAnalysis = analyze(tf, "twocol", "select * |> select * (except c) |> select * (except a)")
    val subAnalysis = analyze(tf, "twocol", "select b |> select b |> select b")

    subAnalysis.statement must be (verticalSliceOf(superAnalysis.statement))
  }

  test("subset - ignores ordering ordering") {
    val tf = tableFinder(
      (0, "twocol") -> D("a" -> TestText, "b" -> TestNumber, "c" -> TestNumber)
    )

    val superAnalysis = analyze(tf, "twocol", "select a, b")
    val subAnalysis = analyze(tf, "twocol", "select b, a")

    subAnalysis.statement must be (verticalSliceOf(superAnalysis.statement))
  }

  test("subset - DISTINCT involved") {
    val tf = tableFinder(
      (0, "multicol") -> D("a" -> TestText, "b" -> TestText, "c" -> TestText, "d" -> TestText, "e" -> TestText)
    )

    val superAnalysis = analyze(tf, "select * from @multicol |> select distinct a, b")
    val subAnalysis = analyze(tf, "select a, b from @multicol |> select distinct a, b")

    subAnalysis.statement must be (verticalSliceOf(superAnalysis.statement))
  }

  test("subset - union all with intermediate full") {
    val tf = tableFinder(
      (0, "twocol1") -> D("a" -> TestText, "b" -> TestNumber),
      (0, "twocol2") -> D("c" -> TestText, "d" -> TestNumber)
    )

    val superAnalysis = analyze(tf, "twocol1", "(select a, b) union all (select c, d from @twocol2)")
    val subAnalysis = analyze(tf, "twocol1", "(select a) union all (select c from @twocol2)")

    subAnalysis.statement must be (verticalSliceOf(superAnalysis.statement))
  }

  test("subset - union all succeed with intermediate slice") {
    val tf = tableFinder(
      (0, "threecol1") -> D("a" -> TestText, "b" -> TestNumber, "c" -> TestNumber),
      (0, "threecol2") -> D("d" -> TestText, "e" -> TestNumber, "f" -> TestNumber)
    )

    val superAnalysis = analyze(tf, "threecol1", "(select a, c) union all (select d, f from @threecol2)")
    val subAnalysis1 = analyze(tf, "threecol1", "(select c) union all (select f from @threecol2)")
    val subAnalysis2 = analyze(tf, "threecol1", "(select b) union all (select e from @threecol2)")

    subAnalysis1.statement must be (verticalSliceOf(superAnalysis.statement))
  }

  test("subset - union all fail") {
    val tf = tableFinder(
      (0, "threecol1") -> D("a" -> TestText, "b" -> TestNumber, "c" -> TestNumber),
      (0, "threecol2") -> D("d" -> TestText, "e" -> TestNumber, "f" -> TestNumber)
    )

    val superAnalysis = analyze(tf, "threecol1", "(select a, c) union all (select d, f from @threecol2)")
    val subAnalysis1 = analyze(tf, "threecol1", "(select c) union all (select e from @threecol2)")
    val subAnalysis2 = analyze(tf, "threecol1", "(select b) union all (select f from @threecol2)")

    subAnalysis1.statement must not be (verticalSliceOf(superAnalysis.statement))
    subAnalysis2.statement must not be (verticalSliceOf(superAnalysis.statement))
  }

  test("subset - union") {
    val tf = tableFinder(
      (0, "threecol1") -> D("a" -> TestText, "b" -> TestNumber, "c" -> TestNumber),
      (0, "threecol2") -> D("d" -> TestText, "e" -> TestNumber, "f" -> TestNumber),
      (0, "qthreecol1") -> Q(0, "threecol1", "select a, b, c"),
      (0, "qthreecol2") -> Q(0, "threecol2", "select d, e, f"),
      (0, "qtwocol1") -> Q(0, "threecol1", "select a, b"),
      (0, "qtwocol2") -> Q(0, "threecol2", "select d, e"),
    )

    val superAnalysis = analyze(tf, "qthreecol1", "(select a, b) union (select d, e from @qthreecol2)")
    val subAnalysis = analyze(tf, "qtwocol1", "(select a, b) union all (select d, e from @qtwocol2)")

    subAnalysis.statement must be (verticalSliceOf(superAnalysis.statement))
  }

  test("unique - join between unique and non-unique") {
    val tf = tableFinder(
      (0, "a") -> D("a1" -> TestText, "a2" -> TestNumber, "a3" -> TestNumber).withPrimaryKey("a1"),
      (0, "b") -> D("b1" -> TestText, "b2" -> TestNumber, "b3" -> TestNumber)
    )

    val analysis = analyze(tf, "a", "select *, @b.* join @b on a1 = @b.b1")
    analysis.statement.unique must be (Nil)
  }

  test("unique - join between multiple uniques") {
    val tf = tableFinder(
      (0, "a") -> D("a1" -> TestText, "a2" -> TestNumber, "a3" -> TestNumber).withPrimaryKey("a1"),
      (0, "b") -> D("b1" -> TestText, "b2" -> TestNumber, "b3" -> TestNumber).withPrimaryKey("b2")
    )

    val analysis = analyze(tf, "a", "select *, @b.* join @b on a1 = @b.b1")
    val uniqueNames = analysis.statement.unique.map { cols =>
      cols.map { col =>
        analysis.statement.schema(col).name
      }
    }
    uniqueNames must be (Seq(Seq(cn("a1"), cn("b2"))))
  }

  test("unique - outer join prevents uniqueness") {
    val tf = tableFinder(
      (0, "a") -> D("a1" -> TestText, "a2" -> TestNumber, "a3" -> TestNumber).withPrimaryKey("a1"),
      (0, "b") -> D("b1" -> TestText, "b2" -> TestNumber, "b3" -> TestNumber).withPrimaryKey("b2")
    )

    val analysis = analyze(tf, "a", "select *, @b.* left outer join @b on a1 = @b.b1")
    val uniqueNames = analysis.statement.unique.map { cols =>
      cols.map { col =>
        analysis.statement.schema(col).name
      }
    }
    uniqueNames must be (Nil)
  }

  test("limit if unlimited - no select") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyzeSaved(tf, "twocol").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "twocol", "select * limit 5")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - simple select") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "twocol", "select *").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "twocol", "select * limit 5")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - simple select with limit") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "twocol", "select * limit 10").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "twocol", "select * limit 10")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - unlimited union") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1) union (select * from @twocol2)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1) union (select * from @twocol2) |> select * limit 5")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - left-limited union") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1 limit 10) union (select * from @twocol2)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1 limit 10) union (select * from @twocol2) |> select * limit 5")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - right-limited union") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1) union (select * from @twocol2 limit 10)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1) union (select * from @twocol2 limit 10) |> select * limit 5")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - bi-limited union") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1 limit 20) union (select * from @twocol2 limit 10)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1 limit 20) union (select * from @twocol2 limit 10)")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - un-limited intersect") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1) intersect (select * from @twocol2)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1) intersect (select * from @twocol2) |> select * limit 5")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - left-limited intersect") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1 limit 10) intersect (select * from @twocol2)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1 limit 10) intersect (select * from @twocol2)")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - right-limited intersect") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1) intersect (select * from @twocol2 limit 10)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1) intersect (select * from @twocol2 limit 10)")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - bi-limited intersect") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1 limit 20) intersect (select * from @twocol2 limit 10)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1 limit 20) union (select * from @twocol2 limit 10)")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - un-limited minus") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1) minus (select * from @twocol2)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1) minus (select * from @twocol2) |> select * limit 5")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - left-limited minus") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1 limit 10) minus (select * from @twocol2)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1 limit 10) minus (select * from @twocol2)")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - right-limited minus") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1) minus (select * from @twocol2 limit 10)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1) minus (select * from @twocol2 limit 10) |> select * limit 5")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("limit if unlimited - bi-limited minus") {
    val tf = tableFinder(
      (0, "twocol1") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "twocol2") -> D("text" -> TestText, "num" -> TestNumber)
    )
    val analysis = analyze(tf, "(select * from @twocol1 limit 20) minus (select * from @twocol2 limit 10)").limitIfUnlimited(nnbi(5))
    val expectedAnalysis = analyze(tf, "(select * from @twocol1 limit 20) union (select * from @twocol2 limit 10)")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove trivial single-row selects - leftmost") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "text" -> TestText,
        "num" -> TestNumber
      )
    )

    val analysis = analyze(tf, "select @table1.text, @table1.num from @single_row join @table1 on true").removeTrivialJoins(isLiteralTrue)
    val expectedAnalysis = analyze(tf, "select @table1.text, @table1.num from @table1")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("don't remove non-trivial single-row selects - leftmost") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "text" -> TestText,
        "num" -> TestNumber
      )
    )

    val analysis = analyze(tf, "select @table1.text, @table1.num from @single_row join @table1 on false").removeTrivialJoins(isLiteralTrue)
    val expectedAnalysis = analyze(tf, "select @table1.text, @table1.num from @single_row join @table1 on false")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove trivial single-row selects - rightmost") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "text" -> TestText,
        "num" -> TestNumber
      )
    )

    val analysis = analyze(tf, "select @table1.text, @table1.num from @table1 join @single_row on true").removeTrivialJoins(isLiteralTrue)
    val expectedAnalysis = analyze(tf, "select @table1.text, @table1.num from @table1")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("don't remove non-trivial single-row selects - rightmost") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "text" -> TestText,
        "num" -> TestNumber
      )
    )

    val analysis = analyze(tf, "select @table1.text, @table1.num from @table1 join @single_row on false").removeTrivialJoins(isLiteralTrue)
    val expectedAnalysis = analyze(tf, "select @table1.text, @table1.num from @table1 join @single_row on false")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove trivial single-row selects - midmost") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "text" -> TestText,
        "num" -> TestNumber
      ),
      (0, "table2") -> D(
        "txet" -> TestText,
        "mun" -> TestNumber
      )
    )

    val analysis = analyze(tf, "select @table1.text, @table1.num, @table2.mun from @table1 join @single_row on true join @table2 on @table1.text = @table2.txet").removeTrivialJoins(isLiteralTrue)
    val expectedAnalysis = analyze(tf, "select @table1.text, @table1.num, @table2.mun from @table1 join @table2 on @table1.text = @table2.txet")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("don't remove non-trivial single-row selects - midmost") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "text" -> TestText,
        "num" -> TestNumber
      ),
      (0, "table2") -> D(
        "txet" -> TestText,
        "mun" -> TestNumber
      )
    )

    val analysis = analyze(tf, "select @table1.text, @table1.num, @table2.mun from @table1 join @single_row on false join @table2 on @table1.text = @table2.txet").removeTrivialJoins(isLiteralTrue)
    val expectedAnalysis = analyze(tf, "select @table1.text, @table1.num, @table2.mun from @table1 join @single_row on false join @table2 on @table1.text = @table2.txet")
    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }
}
