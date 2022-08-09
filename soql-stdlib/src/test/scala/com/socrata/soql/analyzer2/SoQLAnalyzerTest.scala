package com.socrata.soql.analyzer2

import org.scalatest.FunSuite
import org.scalatest.MustMatchers

import org.joda.time.DateTime

import com.socrata.soql.types._
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ResourceName, HoleName}
import com.socrata.soql.functions.{SoQLTypeInfo, SoQLFunctionInfo}

import mocktablefinder._

class SoQLAnalyzerTest extends FunSuite with MustMatchers {
  val analyzer = new SoQLAnalyzer[Int, SoQLType, SoQLValue](SoQLTypeInfo, SoQLFunctionInfo)

  test("simple") {
    val tf = new MockTableFinder(Map.empty)

    val tf.Success(start) = tf.findTables(0, "select ((('5' + 7))), 'hello', '2001-01-01' :: date from @single_row")
    val analysis = analyzer(start, UserParameters.empty)

    println(analysis.statement.debugStr)
  }

  test("untagged parameters") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText.t, "num" -> SoQLNumber.t)),
        (0, "bbbb-bbbb") -> Q(0, "aaaa-aaaa", "select text, num*2, param('gnu')", Map(HoleName("gnu") -> SoQLText))
      )
    )

    val tf.Success(start) = tf.findTables(0, ResourceName("aaaa-aaaa"), "select param('gnu')")
    val analysis = analyzer(start, UserParameters(qualified = Map(CanonicalName("bbbb-bbbb") -> Map(HoleName("gnu") -> Right(SoQLText("Hello world")))), Left(CanonicalName("bbbb-bbbb"))))
    println(analysis.statement.debugStr)
  }

  test("more complex") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText.t, "num" -> SoQLNumber.t)),
        (0, "bbbb-bbbb") -> Q(0, "aaaa-aaaa", "select text, num*2, param(@aaaa-aaaa, 'gnu')"),
        (0, "cccc-cccc") -> U(0, "select ?x from @single_row", OrderedMap("x" -> SoQLText))
      )
    )

    val tf.Success(start) = tf.findTables(0, ResourceName("bbbb-bbbb"), "select @x.*, @t.*, num_2 as bleh from @this as t join lateral @cccc-cccc(text) as x on true")

    val analysis = analyzer(start, UserParameters(qualified = Map(CanonicalName("aaaa-aaaa") -> Map(HoleName("gnu") -> Right(SoQLFixedTimestamp(DateTime.now()))))))

    println(analysis.statement.debugStr)
    // println(analysis.statement.schema.withValuesMapped(_.name))

    // println(analysis.statement.relabel(new LabelProvider(i => s"tbl$i", c => s"col$c")).debugStr)
    // println(analysis.statement.relabel(new LabelProvider(i => s"tbl$i", c => s"col$c")).schema.withValuesMapped(_.name))
  }

  test("aggregates - works") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText.t, "num" -> SoQLNumber.t))
      )
    )

    val tf.Success(start) = tf.findTables(0, ResourceName("aaaa-aaaa"), "select * |> select text, sum(num) as s group by text order by s desc |> select *")

    val analysis = analyzer(start, UserParameters.empty)
    println(analysis.statement.numericate.debugStr)
    println(analysis.statement.schema.withValuesMapped(_.name))
  }
}
