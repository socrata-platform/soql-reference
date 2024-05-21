package com.socrata.soql.stdlib.analyzer2.rollup

import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.analyzer2.rollup.RollupInfo
import com.socrata.soql.environment.{Provenance, ResourceName, ScopedResourceName}
import com.socrata.soql.functions.{SoQLFunctions, MonomorphicFunction, SoQLTypeInfo, SoQLFunctionInfo}
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider

class SoQLFunctionSubsetTest extends SoQLRollupTestHelper {
  test("ymd -> ym subset") {
    val tf = MockTableFinder[MT](
      (0, "dataset") -> D("floating_timestamp" -> SoQLFloatingTimestamp, "number" -> SoQLNumber),
      (1, "rollup") -> D("c1" -> SoQLFloatingTimestamp, "c2" -> SoQLNumber).withPrimaryKey("c1")
    )

    val analysis = analyze(tf, "select date_trunc_ym(floating_timestamp) as month, sum(number) from @dataset group by month")
    val rollup = SoQLRollupInfo(1, "rollup", tf, "select date_trunc_ymd(floating_timestamp) as date, sum(number) from @dataset group by date")
    val Some(result) = rewriter(analysis.statement.asInstanceOf[Select], rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select date_trunc_ym(c1) as y, sum(c2) from @rollup group by y", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("ymd -> y subset") {
    val tf = MockTableFinder[MT](
      (0, "dataset") -> D("floating_timestamp" -> SoQLFloatingTimestamp, "number" -> SoQLNumber),
      (1, "rollup") -> D("c1" -> SoQLFloatingTimestamp, "c2" -> SoQLNumber).withPrimaryKey("c1")
    )

    val analysis = analyze(tf, "select date_trunc_y(floating_timestamp) as month, sum(number) from @dataset group by month")
    val rollup = SoQLRollupInfo(1, "rollup", tf, "select date_trunc_ymd(floating_timestamp) as date, sum(number) from @dataset group by date")
    val Some(result) = rewriter(analysis.statement.asInstanceOf[Select], rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select date_trunc_y(c1) as y, sum(c2) from @rollup group by y", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("ymd -> date subset") {
    val tf = MockTableFinder[MT](
      (0, "dataset") -> D("floating_timestamp" -> SoQLFloatingTimestamp, "number" -> SoQLNumber),
      (1, "rollup") -> D("c1" -> SoQLFloatingTimestamp, "c2" -> SoQLNumber).withPrimaryKey("c1")
    )

    val analysis = analyze(tf, "select floating_timestamp.date as date, sum(number) from @dataset group by date")
    val rollup = SoQLRollupInfo(1, "rollup", tf, "select date_trunc_ymd(floating_timestamp) as date, sum(number) from @dataset group by date")
    val Some(result) = rewriter(analysis.statement.asInstanceOf[Select], rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1.date as date, sum(c2) from @rollup group by date", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("datez subset") {
    val tf = MockTableFinder[MT](
      (0, "dataset") -> D("fixed_timestamp" -> SoQLFixedTimestamp, "number" -> SoQLNumber),
      (1, "rollup") -> D("c1" -> SoQLFixedTimestamp, "c2" -> SoQLNumber).withPrimaryKey("c1")
    )

    val analysis = analyze(tf, "select datez_trunc_y(fixed_timestamp) as year, sum(number) from @dataset group by year")
    val rollup = SoQLRollupInfo(1, "rollup", tf, "select datez_trunc_ym(fixed_timestamp) as month, sum(number) from @dataset group by month")
    val Some(result) = rewriter(analysis.statement.asInstanceOf[Select], rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select datez_trunc_y(c1) as y, sum(c2) from @rollup group by y", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }
}
