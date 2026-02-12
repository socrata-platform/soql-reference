package com.socrata.soql.sqlizer

import scala.util.parsing.input.NoPosition

import org.scalatest.{FunSuite, MustMatchers}

import com.rojoma.json.v3.ast.JString

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.sqlizer._

class SqlizerTest extends FunSuite with MustMatchers with TestHelper with SqlizerUniverse[TestHelper.TestMT] {
  def analyze(tf: TableFinder[TestMT], soql: String, passes: Seq[rewrite.AnyPass] = Nil): Doc = {
    val ft =
      tf.findTables(0, soql, Map.empty) match {
        case Right(ft) => ft
        case Left(err) => fail("Bad query: " + err)
      }

    val analysis =
      analyzer(ft, UserParameters.empty) match {
        case Right(an) => an.applyPasses(passes, TestRewritePassHelpers)
        case Left(err) => fail("Bad query: " + err)
      }

    sqlizer(analysis, TestExtraContext).getOrElse { fail("analysis failed") }.sql
  }

  test("simple") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID,
        "text" -> TestText,
        "num" -> TestNumber,
        "compound" -> TestCompound
      )
    )

    val soql = "select * from @table1"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT x1.text AS i1, x1.num AS i2, x1.compound_a AS i3_a, x1.compound_b AS i3_b FROM table1 AS x1")
  }

  test("simple-positions") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID,
        "text" -> TestText,
        "num" -> TestNumber,
        "compound" -> TestCompound
      )
    )

    val soql = "select num * 2 as n2 from @table1 order by n2"
    val laidOut = analyze(tf, soql).layoutSingleLine
    val sqlish = laidOut.toString
    val poses = Sqlizer.positionInfo(laidOut)

    sqlish must equal ("SELECT (x1.num) * (2.0 :: numeric) AS i1 FROM table1 AS x1 ORDER BY (x1.num) * (2.0 :: numeric) ASC NULLS LAST")
    val positions =    "_______AAAAAAAAAAAABBBBBBBBBBBBBBA__________________________________AAAAAAAAAAAABBBBBBBBBBBBBBA_______________"
    sqlish.length must equal(positions.length) // just a sanity check

    poses.length must equal (sqlish.length)
    poses.zip(positions).foreach {
      case (p, '_') if p.position == NoPosition => // ok
      case (p, 'A') => (p.position.line, p.position.column) must equal ((1, 8))
      case (p, 'B') => (p.position.line, p.position.column) must equal ((1, 14))
      case (other, c) => fail(s"Found specifier $c, but got position ${other.position}")
    }
  }

  test("table op - no compression") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID,
        "text" -> TestText,
        "num" -> TestNumber,
        "compound" -> TestCompound
      ),
      (0, "table2") -> D(
        "number" -> TestNumber,
        "other_compound" -> TestCompound
      )
    )

    val soql = "select num, compound from @table1 union select number, other_compound from @table2"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString

    sqlish must equal ("(SELECT x1.num AS i1, x1.compound_a AS i2_a, x1.compound_b AS i2_b FROM table1 AS x1) UNION (SELECT x3.number AS i3, x3.other_compound_a AS i4_a, x3.other_compound_b AS i4_b FROM table2 AS x3)")
  }

  test("table op - left compression induces right compression") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID,
        "text" -> TestText,
        "num" -> TestNumber,
        "compound" -> TestCompound
      ),
      (0, "table2") -> D(
        "number" -> TestNumber,
        "other_compound" -> TestCompound
      )
    )

    val soql = "select num, compress(compound) from @table1 union select number, other_compound from @table2"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString

    sqlish must equal ("(SELECT x1.num AS i1, test_soql_compress_compound(x1.compound_a, x1.compound_b) AS i2 FROM table1 AS x1) UNION (SELECT g1.i3 AS i3, test_soql_compress_compound(g1.i4_a, g1.i4_b) AS i4 FROM (SELECT x3.number AS i3, x3.other_compound_a AS i4_a, x3.other_compound_b AS i4_b FROM table2 AS x3) AS g1)")
  }

  test("table op - right compression induces left compression") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID,
        "text" -> TestText,
        "num" -> TestNumber,
        "compound" -> TestCompound
      ),
      (0, "table2") -> D(
        "number" -> TestNumber,
        "other_compound" -> TestCompound
      )
    )

    val soql = "select num, compound from @table1 union select number, compress(other_compound) from @table2"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString

    sqlish must equal ("(SELECT g1.i1 AS i1, test_soql_compress_compound(g1.i2_a, g1.i2_b) AS i2 FROM (SELECT x1.num AS i1, x1.compound_a AS i2_a, x1.compound_b AS i2_b FROM table1 AS x1) AS g1) UNION (SELECT x3.number AS i3, test_soql_compress_compound(x3.other_compound_a, x3.other_compound_b) AS i4 FROM table2 AS x3)")
  }

  test("table op - both compression induces nothing compression") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID,
        "text" -> TestText,
        "num" -> TestNumber,
        "compound" -> TestCompound
      ),
      (0, "table2") -> D(
        "number" -> TestNumber,
        "other_compound" -> TestCompound
      )
    )

    val soql = "select num, compress(compound) from @table1 union select number, compress(other_compound) from @table2"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString

    sqlish must equal ("(SELECT x1.num AS i1, test_soql_compress_compound(x1.compound_a, x1.compound_b) AS i2 FROM table1 AS x1) UNION (SELECT x3.number AS i3, test_soql_compress_compound(x3.other_compound_a, x3.other_compound_b) AS i4 FROM table2 AS x3)")
  }

  test("provenance - simple") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID
      )
    )

    val soql = "select :id from @table1 where :id = 'row-qwer-tyui-2345'"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT text \"table1\" AS i1_provenance, x1.:id AS i1 FROM table1 AS x1 WHERE (x1.:id) = (1200459281559959 :: bigint)")
  }

  test("provenance - joined") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID
      ),
      (0, "table2") -> D(
        ":id" -> TestID
      )
    )

    val soql = "select :id from @table1 join @table2 on @table1.:id = @table2.:id"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT text \"table1\" AS i1_provenance, x1.:id AS i1 FROM table1 AS x1 JOIN table2 AS x2 ON ((text \"table1\") IS NOT DISTINCT FROM (text \"table2\")) AND ((x1.:id) = (x2.:id))")
  }

  test("provenance - compressed") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID
      )
    )

    val soql = "select :id from @table1 where :id = compress('row-qwer-tyui-2345')"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT text \"table1\" AS i1_provenance, x1.:id AS i1 FROM table1 AS x1 WHERE (test_soql_compress_compound(text \"table1\", x1.:id)) = (test_soql_compress_compound(text \"table1\", 1200459281559959 :: bigint))")
  }

  test("provenance - order by physical column does not include the provenance") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID
      )
    )

    val soql = "select :id from @table1 order by :id"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT text \"table1\" AS i1_provenance, x1.:id AS i1 FROM table1 AS x1 ORDER BY x1.:id ASC NULLS LAST")
  }

  test("provenance - order by simple logical column does not include the provenance") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID
      )
    )

    val soql = "select :id from @table1 |> select :id order by :id"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT x2.i1_provenance AS i2_provenance, x2.i1 AS i2 FROM (SELECT text \"table1\" AS i1_provenance, x1.:id AS i1 FROM table1 AS x1) AS x2 ORDER BY x2.i1 ASC NULLS LAST")
  }

  test("provenance - order by unioned logical column DOES include the provenance") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID
      ),
      (0, "table2") -> D(
        ":id" -> TestID
      )
    )

    val soql = "(select :id from @table1 union select :id from @table2) |> select :id order by :id"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT x5.i1_provenance AS i3_provenance, x5.i1 AS i3 FROM ((SELECT text \"table1\" AS i1_provenance, x1.:id AS i1 FROM table1 AS x1) UNION (SELECT text \"table2\" AS i2_provenance, x3.:id AS i2 FROM table2 AS x3)) AS x5 ORDER BY x5.i1_provenance ASC NULLS LAST, x5.i1 ASC NULLS LAST")
  }

  test("provenance - order by compressed") {
    val tf = tableFinder(
      (0, "table1") -> D(":id" -> TestID)
    )

    val soql = "select :id from @table1 order by compress(:id)"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""SELECT text "table1" AS i1_provenance, x1.:id AS i1 FROM table1 AS x1 ORDER BY test_soql_compress_compound(text "table1", x1.:id) ASC NULLS LAST""")
  }

  test("table function - trivial") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "txt" -> TestText,
        "category" -> TestNumber
      )
    )

    val soql = "select window_function(txt) over () from @table1"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT windowed_function(x1.txt) OVER () AS i1 FROM table1 AS x1")
  }

  test("table function - no bounds") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "txt" -> TestText,
        "category" -> TestNumber
      )
    )

    val soql = "select window_function(txt) over (partition by category order by txt) from @table1"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT windowed_function(x1.txt) OVER (PARTITION BY x1.category ORDER BY x1.txt ASC NULLS LAST) AS i1 FROM table1 AS x1")
  }

  test("table function - single bound") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "txt" -> TestText,
        "category" -> TestNumber
      )
    )

    val soql = "select window_function(txt) over (partition by category order by txt rows 5 preceding) from @table1"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT windowed_function(x1.txt) OVER (PARTITION BY x1.category ORDER BY x1.txt ASC NULLS LAST ROWS 5 PRECEDING) AS i1 FROM table1 AS x1")
  }

  test("table function - double single bound") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "txt" -> TestText,
        "category" -> TestNumber
      )
    )

    val soql = "select window_function(txt) over (partition by category order by txt rows between 5 preceding and 5 following) from @table1"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT windowed_function(x1.txt) OVER (PARTITION BY x1.category ORDER BY x1.txt ASC NULLS LAST ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS i1 FROM table1 AS x1")
  }

  test("trivial from is eliminated") {
    val tf = tableFinder()
    val soql = "select 1, 2 from @single_row"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT 1.0 :: numeric AS i1, 2.0 :: numeric AS i2")
  }

  test("order by a selected function call which produces a non-compressed compound column") {
    val tf = tableFinder()
    val soql = "select expanded_compound() as ec from @single_row order by ec"
    val sqlish = analyze(tf, soql, Seq(rewrite.Pass.UseSelectListReferences)).layoutSingleLine.toString

    // and in particular _not_ "select "c a", "c b" order by compress(1, 2)"...
    sqlish must equal ("""SELECT "column a" AS i1_a, "column b" AS i1_b ORDER BY test_soql_compress_compound("column a", "column b") ASC NULLS LAST""")
  }

  test("selecting nothing from @single_row turns into 'select 1'") {
    val tf = tableFinder()
    val soql = "select from @single_row"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT 1")
  }

  test("selecting nothing from a table turns into 'select 1 from that_table'") {
    val tf = tableFinder(
      (0, "table1") -> D(
        ":id" -> TestID
      )
    )
    val soql = "select from @table1"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("SELECT 1 FROM table1 AS x1")
  }

  test("search, text only") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestText
      )
    )
    val soql = "select from @table1 search 'foo'"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""SELECT 1 FROM table1 AS x1 WHERE search(prepare_haystack(((coalesce(x1.a, text "")) || (text " ")) || (coalesce(x1.b, text ""))), prepare_needle(text "foo"))""")
  }

  test("search, text and number but non-numeric needle") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestText,
        "c" -> TestNumber
      )
    )
    val soql = "select from @table1 search 'foo'"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""SELECT 1 FROM table1 AS x1 WHERE search(prepare_haystack(((coalesce(x1.a, text "")) || (text " ")) || (coalesce(x1.b, text ""))), prepare_needle(text "foo"))""")
  }

  test("search, text and number with numeric needle") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestText,
        "c" -> TestNumber,
        "d" -> TestNumber
      )
    )
    val soql = "select from @table1 search '1'"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""SELECT 1 FROM table1 AS x1 WHERE ((search(prepare_haystack(((coalesce(x1.a, text "")) || (text " ")) || (coalesce(x1.b, text ""))), prepare_needle(text "1"))) OR ((x1.c) = (1.0 :: numeric))) OR ((x1.d) = (1.0 :: numeric))""")
  }

  test("search, number only with numeric needle") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "c" -> TestNumber,
        "d" -> TestNumber
      )
    )
    val soql = "select from @table1 search '1'"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""SELECT 1 FROM table1 AS x1 WHERE ((x1.c) = (1.0 :: numeric)) OR ((x1.d) = (1.0 :: numeric))""")
  }

  test("search, number only with non-numeric needle") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "c" -> TestNumber,
        "d" -> TestNumber
      )
    )
    val soql = "select from @table1 search 'hello'"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""SELECT 1 FROM table1 AS x1 WHERE false""")
  }

  test("search, pushdown") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestText,
        "c" -> TestText
      ),
      (0, "view1") -> Q(
        0, "table1",
        "select a, b where a = b order by a"
      )
    )
    val soql = "select from @view1 search 'foo'"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""SELECT 1 FROM (SELECT x1.a AS i1, x1.b AS i2 FROM table1 AS x1 WHERE (search(prepare_haystack(((((coalesce(x1.a, text "")) || (text " ")) || (coalesce(x1.b, text ""))) || (text " ")) || (coalesce(x1.c, text ""))), prepare_needle(text "foo"))) AND ((x1.a) = (x1.b)) ORDER BY x1.a ASC NULLS LAST) AS x2 WHERE search(prepare_haystack(((coalesce(x2.i1, text "")) || (text " ")) || (coalesce(x2.i2, text ""))), prepare_needle(text "foo"))""")
  }

  test("search, pushdown blocked by grouping") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestText,
        "c" -> TestText
      ),
      (0, "view1") -> Q(
        0, "table1",
        "select a, b group by a, b"
      )
    )
    val soql = "select from @view1 search 'foo'"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""SELECT 1 FROM (SELECT x1.a AS i1, x1.b AS i2 FROM table1 AS x1 GROUP BY x1.a, x1.b) AS x2 WHERE search(prepare_haystack(((coalesce(x2.i1, text "")) || (text " ")) || (coalesce(x2.i2, text ""))), prepare_needle(text "foo"))""")
  }

  test("search, pushdown blocked by nontrivial function") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestText,
        "c" -> TestText
      ),
      (0, "view1") -> Q(
        0, "table1",
        "select a || b"
      )
    )
    val soql = "select from @view1 search 'foo'"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""SELECT 1 FROM (SELECT (x1.a) || (x1.b) AS i1 FROM table1 AS x1) AS x2 WHERE search(prepare_haystack(coalesce(x2.i1, text "")), prepare_needle(text "foo"))""")
  }

  test("union sqlizes as UNION") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestNumber
      ),
      (0, "table2") -> D(
        "c" -> TestText,
        "d" -> TestNumber
      )
    )
    val soql = "(select * from @table1) union (select * from @table2)"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""(SELECT x1.a AS i1, x1.b AS i2 FROM table1 AS x1) UNION (SELECT x3.c AS i3, x3.d AS i4 FROM table2 AS x3)""")
  }

  test("union all sqlizes as UNION ALL") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestNumber
      ),
      (0, "table2") -> D(
        "c" -> TestText,
        "d" -> TestNumber
      )
    )
    val soql = "(select * from @table1) union all (select * from @table2)"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""(SELECT x1.a AS i1, x1.b AS i2 FROM table1 AS x1) UNION ALL (SELECT x3.c AS i3, x3.d AS i4 FROM table2 AS x3)""")
  }

  test("intersect sqlizes as INTERSECT") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestNumber
      ),
      (0, "table2") -> D(
        "c" -> TestText,
        "d" -> TestNumber
      )
    )
    val soql = "(select * from @table1) intersect (select * from @table2)"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""(SELECT x1.a AS i1, x1.b AS i2 FROM table1 AS x1) INTERSECT (SELECT x3.c AS i3, x3.d AS i4 FROM table2 AS x3)""")
  }

  test("intersect all sqlizes as INTERSECT ALL") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestNumber
      ),
      (0, "table2") -> D(
        "c" -> TestText,
        "d" -> TestNumber
      )
    )
    val soql = "(select * from @table1) intersect all (select * from @table2)"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""(SELECT x1.a AS i1, x1.b AS i2 FROM table1 AS x1) INTERSECT ALL (SELECT x3.c AS i3, x3.d AS i4 FROM table2 AS x3)""")
  }

  test("minus sqlizes as EXCEPT") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestNumber
      ),
      (0, "table2") -> D(
        "c" -> TestText,
        "d" -> TestNumber
      )
    )
    val soql = "(select * from @table1) minus (select * from @table2)"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""(SELECT x1.a AS i1, x1.b AS i2 FROM table1 AS x1) EXCEPT (SELECT x3.c AS i3, x3.d AS i4 FROM table2 AS x3)""")
  }

  test("minus all sqlizes as EXCEPT ALL") {
    val tf = tableFinder(
      (0, "table1") -> D(
        "a" -> TestText,
        "b" -> TestNumber
      ),
      (0, "table2") -> D(
        "c" -> TestText,
        "d" -> TestNumber
      )
    )
    val soql = "(select * from @table1) minus all (select * from @table2)"
    val sqlish = analyze(tf, soql).layoutSingleLine.toString
    sqlish must equal ("""(SELECT x1.a AS i1, x1.b AS i2 FROM table1 AS x1) EXCEPT ALL (SELECT x3.c AS i3, x3.d AS i4 FROM table2 AS x3)""")
  }

  test("CTE") {
    val tf = tableFinder(
      (0, "table") -> D(
        "id" -> TestNumber,
        "text" -> TestText,
        "num" -> TestNumber
      ),
      (0, "inner_query") -> Q(0, "table", "select id, text"),
      (0, "outer_query") -> Q(0, "inner_query", "select text as t1, @q.text as t2 join @inner_query as @q on true")
    )

    val soql = "select @q1.t1, @q2.t2 from @outer_query as @q1 join @outer_query as @q2 on true"

    val sqlish = analyze(tf, soql, Seq(rewrite.Pass.MaterializeNamedQueries)).layoutSingleLine.toString

    sqlish must equal ("""WITH c1 AS (SELECT x1.id AS i1, x1.text AS i2 FROM table AS x1), c2 AS (SELECT x2.i2 AS i5, x4.i2 AS i6 FROM c1 AS x2 JOIN c1 AS x4 ON true) SELECT x5.i5 AS i13, x10.i6 AS i14 FROM c2 AS x5 JOIN c2 AS x10 ON true""")
  }

  test("CTE - uncompressed column") {
    val tf = tableFinder(
      (0, "table") -> D(
        "compound" -> TestCompound
      ),
      (0, "query") -> Q(0, "table", "select compound as c")
    )

    val soql = "select @q1.c, @q2.c as c2 from @query as @q1 join @query as @q2 on true"

    val sqlish = analyze(tf, soql, Seq(rewrite.Pass.MaterializeNamedQueries)).layoutSingleLine.toString

    sqlish must equal ("""WITH c1 AS (SELECT x1.compound_a AS i1_a, x1.compound_b AS i1_b FROM table AS x1) SELECT x2.i1_a AS i3_a, x2.i1_b AS i3_b, x4.i1_a AS i4_a, x4.i1_b AS i4_b FROM c1 AS x2 JOIN c1 AS x4 ON true""")
  }

  test("CTE - compressed column") {
    val tf = tableFinder(
      (0, "table") -> D(
        "compound" -> TestCompound
      ),
      (0, "query") -> Q(0, "table", "select compress(compound) as c")
    )

    val soql = "select @q1.c, @q2.c as c2 from @query as @q1 join @query as @q2 on true"

    val sqlish = analyze(tf, soql, Seq(rewrite.Pass.MaterializeNamedQueries)).layoutSingleLine.toString

    sqlish must equal ("""WITH c1 AS (SELECT test_soql_compress_compound(x1.compound_a, x1.compound_b) AS i1 FROM table AS x1) SELECT x2.i1 AS i3, x4.i1 AS i4 FROM c1 AS x2 JOIN c1 AS x4 ON true""")
  }
}
