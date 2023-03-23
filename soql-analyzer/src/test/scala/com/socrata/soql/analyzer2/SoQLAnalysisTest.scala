package com.socrata.soql.analyzer2

import scala.util.parsing.input.NoPosition

import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.soql.collection._
import com.socrata.soql.serialize.{ReadBuffer, WriteBuffer}

import mocktablefinder._

class SoQLAnalysisTest extends FunSuite with MustMatchers with TestHelper {
  val rowNumber = TestFunctions.RowNumber.monomorphic.get
  val windowFunction = TestFunctions.WindowFunction.monomorphic.get
  val and = TestFunctions.And.monomorphic.get

  test("direct query does not add a column") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by text")
    analysis.preserveOrdering.statement.schema.size must equal (2)
  }

  test("unordered-on-unordered doesn't change under order preservation") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * |> select *")
    analysis.preserveOrdering.statement must equal (analysis.statement)
  }

  test("unordered-on-ordered pipes does not generate an ordering column if unnecessary") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num |> select *")
    val expectedAnalysis = analyze(tf, "twocol", "select * order by num |> select * order by num")
    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("unordered-on-ordered pipes generates an ordering column if necessary") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num+1 |> select *")
    val expectedAnalysis = analyze(tf, "twocol", "select *, num+1 as ordering order by num+1 |> select * (except ordering) order by ordering")
    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("ordered-on-ordered pipes does not generate an ordering column if unnecessary") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num |> select * order by text")
    val expectedAnalysis = analyze(tf, "twocol", "select * order by num |> select * order by text, num")
    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("ordered-on-ordered pipes generates an ordering column") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by num+1 |> select * order by text")
    val expectedAnalysis = analyze(tf, "twocol", "select *, num+1 as ordering order by num+1 |> select * (except ordering) order by text, ordering")
    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("orderings are blocked by aggregations and do not continue beyond them if not required by a window function") {
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

  test("orderings are blocked by aggregations but continue again beyond them if required") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", """
select * order by num+1
  |> select *
  |> select *, window_function() over ()
  |> select text, num group by text, num order by num+2, text+'x'
  |> select *
""")

    val expectedAnalysis = analyze(tf, "twocol", """
select *, num+1 as ordering order by num+1
  |> select * order by ordering
  |> select * (except ordering), window_function() over () order by ordering
  |> select text, num, num+2 as o1, text+'x' as o2 group by text, num order by num+2, text+'x'
  |> select * (except o1, o2) order by o1, o2
""")

    analysis.preserveOrdering.statement must be (isomorphicTo(expectedAnalysis.statement))
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

    val analysis = analyze(tf, "twocol", "select text, num order by text |> select text, num order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num |> select text, num order by num")

    analysis.removeUnusedOrderBy.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("remove unused order by - keep ordering when there's a window function involved") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num, window_function() over () order by text |> select text, num order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num, window_function() over () order by text |> select text, num order by num")

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

  test("preserve ordering + remove unused order by - keep ordering only at the top level") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num order by text |> select text, num order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num |> select text, num order by num, text")

    analysis.preserveOrdering.removeUnusedOrderBy.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - primary key") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select text, num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num order by :id")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - group by") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select text, sum(num) as sum group by text order by sum desc")
    val expectedAnalysis = analyze(tf, "twocol", "select text, sum(num) group by text order by sum(num) desc, text")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - group by + inherited PK") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber, "unorderable" -> TestUnorderable).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select unorderable, :id, sum(num) as sum group by unorderable, :id order by sum desc")
    val expectedAnalysis = analyze(tf, "twocol", "select unorderable, :id, sum(num) group by unorderable, :id order by sum(num) desc, :id")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - partial distinct on") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select distinct on (text, num*2) text, num order by text desc")
    val expectedAnalysis = analyze(tf, "twocol", "select distinct on (text, num*2) text, num order by text desc, num*2, num")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
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

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
    analysis.preserveOrdering.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysisWithPreservation.statement))
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

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
    analysis.preserveOrdering.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysisWithPreservation.statement))
  }

  test("impose ordering - non-distinct order by") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select distinct on (text) text, num order by text, num*3 desc")
    val expectedAnalysis = analyze(tf, "twocol", "select distinct on (text) text, num order by text, num*3 desc, num")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - indistinct") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*2 order by text, num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*2 order by text, num, num*2")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - join") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id"),
      (0, "threecol") -> D(":id" -> TestNumber, "a" -> TestText, "b" -> TestText, "c" -> TestText).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select text, num*2 join @threecol on true order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*2 join @threecol on true order by num, :id, @threecol.:id")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - union") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "altcol") -> D("words" -> TestText, "digits" -> TestNumber),
      // Can't put a subselect in initial-FROM position in soql
      // directly, so we need a saved query to use for that
      (0, "union") -> Q(0, "twocol", "select * union select * from @altcol")
    )

    val analysis = analyze(tf, "twocol", "select * union select * from @altcol")
    val expectedAnalysis = analyze(tf, "union", "select * order by text, num")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - unorderable") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "another_text" -> TestUnorderable)
    )

    val analysis = analyze(tf, "threecol", "select *")
    val expectedAnalysis = analyze(tf, "threecol", "select * order by text, num")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - across stages") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select text, num, num + num as num2 |> select text, num2 order by num2")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num, num+num, :id |> select text, num_num order by num_num, :id")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - join and follow-up stage") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id"),
      (0, "threecol") -> D(":id" -> TestNumber, "a" -> TestText, "b" -> TestText, "c" -> TestText).withPrimaryKey(":id")
    )

    val analysis = analyze(tf, "twocol", "select *, @threecol.* join @threecol on true |> select text, b order by num")
    val expectedAnalysis = analyze(tf, "twocol", "select *, @threecol.*, :id, @threecol.:id as tcid join @threecol on true |> select text, b order by num, :id, tcid")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - multiple primary keys") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id").withPrimaryKey("text")
    )

    val analysis = analyze(tf, "twocol", "select num |> select 1")
    val expectedAnalysis = analyze(tf, "twocol", "select num, :id, text |> select 1 order by :id")

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("impose ordering - already selected") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber).withPrimaryKey(":id").withPrimaryKey("text")
    )

    val analysis = analyze(tf, "twocol", "select text, num |> select 1")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num, :id |> select 1 order by :id") // it'll choose :id to order by just because it comes first

    analysis.imposeOrdering(TestTypeInfo.isOrdered).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - combined tables") {
    val tf = tableFinder(
      (0, "a") -> D("a1" -> TestText, "a2" -> TestNumber),
      (0, "b") -> D("b1" -> TestText, "b2" -> TestNumber)
    )

    val analysis = analyze(tf, "(select * from @a) union (select * from @b)")
    val expectedAnalysis = analyze(tf, "((select * from @a) union (select * from @b)) |> select * limit 25 offset 50")
    analysis.addLimitOffset(limit = Some(25), offset = Some(50)).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - no limit/offset initially") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 25 offset 50")
    analysis.addLimitOffset(limit = Some(25), offset = Some(50)).statement must be (isomorphicTo(expectedAnalysis.statement))
  }


  test("addLimitOFfset - fits within initial limit/offset") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 25 offset 150")
    analysis.addLimitOffset(limit = Some(25), offset = Some(50)).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - overlaps the end of initial limit/offset") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 50 offset 150")
    analysis.addLimitOffset(limit = Some(500), offset = Some(50)).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - overlaps the end of initial limit/offset with no new limit provided") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 50 offset 150")
    analysis.addLimitOffset(limit = None, offset = Some(50)).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - passes the end of initial limit/offset") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 0 offset 200") // Note the offset will not pass the end of the original chunk
    analysis.addLimitOffset(limit = Some(500), offset = Some(500)).statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("addLimitOffset - passes the end of initial limit/offset with no new limit provided") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select text, num*num limit 100 offset 100")
    val expectedAnalysis = analyze(tf, "twocol", "select text, num*num limit 0 offset 200") // Note the offset will not pass the end of the original chunk
    analysis.addLimitOffset(limit = None, offset = Some(500)).statement must be (isomorphicTo(expectedAnalysis.statement))
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
}
