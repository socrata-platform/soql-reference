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
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText.t, "num" -> SoQLNumber.t)),
        (0, "bbbb-bbbb") -> Q(0, "aaaa-aaaa", "select text, num*2, param(@aaaa-aaaa, 'gnu')"),
        (0, "cccc-cccc") -> U(0, "select ?x from @single_row", OrderedMap("x" -> SoQLText))
      )
    )

    val tf.Success(start) = tf.findTables(0, ResourceName("bbbb-bbbb"), "select @cccc-cccc.*, @t.*, num_2 as bleh from @this as t join lateral @cccc-cccc(text) on true")

    val analysis = analyzer(start, Map("aaaa-aaaa" -> Map(HoleName("gnu") -> Right(SoQLFixedTimestamp(DateTime.now())))))

    println(analysis.statement.debugStr)
    println(analysis.statement.schema.withValuesMapped(_.name))
  }
}
