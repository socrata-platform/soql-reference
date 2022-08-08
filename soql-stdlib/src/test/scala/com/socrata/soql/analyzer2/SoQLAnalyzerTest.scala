package com.socrata.soql.analyzer2

import org.scalatest.FunSuite
import org.scalatest.MustMatchers

import com.socrata.soql.types._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.{SoQLTypeInfo, SoQLFunctionInfo}

import mocktablefinder._

class SoQLAnalyzerTest extends FunSuite with MustMatchers {
  val analyzer = new SoQLAnalyzer[Int, SoQLType, SoQLValue](SoQLTypeInfo, SoQLFunctionInfo)

  test("simple") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText.t, "num" -> SoQLNumber.t))
      )
    )

    val tf.Success(start) = tf.findTables(0, ResourceName("aaaa-aaaa"), "select text, num*2")

    println(analyzer(start, Map.empty).statement)
  }
}
