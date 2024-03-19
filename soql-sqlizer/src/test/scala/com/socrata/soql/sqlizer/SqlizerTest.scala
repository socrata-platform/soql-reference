package com.socrata.soql.sqlizer

import scala.util.parsing.input.NoPosition

import org.scalatest.{FunSuite, MustMatchers}

import com.rojoma.json.v3.ast.JString

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.sqlizer._

class SqlizerTest extends FunSuite with MustMatchers with TestHelper with SqlizerUniverse[TestHelper.TestMT] {
  def analyze(tf: TableFinder[TestMT], soql: String): Doc = {
    val ft =
      tf.findTables(0, soql, Map.empty) match {
        case Right(ft) => ft
        case Left(err) => fail("Bad query: " + err)
      }

    val analysis =
      analyzer(ft, UserParameters.empty) match {
        case Right(an) => an
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
}
