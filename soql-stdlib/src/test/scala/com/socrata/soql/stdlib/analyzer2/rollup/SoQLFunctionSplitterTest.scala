package com.socrata.soql.stdlib.analyzer2.rollup

import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.analyzer2.rollup.RollupInfo
import com.socrata.soql.environment.{Provenance, ResourceName, ScopedResourceName}
import com.socrata.soql.functions.{SoQLFunctions, MonomorphicFunction, SoQLTypeInfo, SoQLFunctionInfo}
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider

class SoQLFunctionSplitterTest extends SoQLRollupTestHelper {
  test("avg") {
    val tf = MockTableFinder[MT](
      (0, "dataset") -> D("text" -> SoQLText, "number" -> SoQLNumber),
      (1, "rollup") -> D("c1" -> SoQLText, "c2" -> SoQLNumber, "c3" -> SoQLNumber).withPrimaryKey("c1")
    )

    val analysis = analyze(tf, "select avg(number) from @dataset")
    val rollup = SoQLRollupInfo(1, "rollup", tf, "select text, sum(number), count(number) from @dataset group by text")
    val Some(result) = rollupExact(analysis.statement.asInstanceOf[Select], rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select sum(c2)/coalesce(sum(c3), 0) from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }
}
