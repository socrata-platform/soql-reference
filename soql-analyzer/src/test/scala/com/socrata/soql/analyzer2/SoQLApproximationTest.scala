package com.socrata.soql.analyzer2

import org.scalatest.{FunSuite, MustMatchers}

import mocktablefinder._

class SoQLApproximationTest extends FunSuite with MustMatchers with TestHelper {
  test("simple") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "twocol", "select * order by text")
    val soql = SoQLApproximation(analysis, TestSoQLApproximationMetaOps)
    val analysis2 = analyze(tf, soql.toString)
    analysis.statement must be (isomorphicTo(analysis2.statement))
  }

  test("query on saved") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "saved") -> Q(0, "twocol", "select * order by text")
    )

    val analysis = analyze(tf, "saved", "select text, -num")
    val soql = SoQLApproximation(analysis, TestSoQLApproximationMetaOps)
    val analysis2 = analyze(tf, soql.toString)
    analysis.statement must be (isomorphicTo(analysis2.statement))
  }

  test("join") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "othercol") -> D("num" -> TestNumber, "text2" -> TestText)
    )

    val analysis = analyze(tf, "twocol", "select text, @o.text2 join @othercol as @o on num = @o.num")
    val soql = SoQLApproximation(analysis, TestSoQLApproximationMetaOps)
    val analysis2 = analyze(tf, soql.toString)
    analysis.statement must be (isomorphicTo(analysis2.statement))
  }

  test("join on saved") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "saved") -> Q(0, "twocol", "select * order by text"),
      (0, "othercol") -> D("num" -> TestNumber, "text2" -> TestText)
    )

    val analysis = analyze(tf, "saved", "select text, @o.text2 join @othercol as @o on num = @o.num")
    val soql = SoQLApproximation(analysis, TestSoQLApproximationMetaOps)
    val analysis2 = analyze(tf, soql.toString)
    analysis.statement must be (isomorphicTo(analysis2.statement))
  }
}
