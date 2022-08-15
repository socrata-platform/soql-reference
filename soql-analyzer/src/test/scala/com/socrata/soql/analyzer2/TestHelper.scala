package com.socrata.soql.analyzer2

import org.scalatest.matchers.{BeMatcher, MatchResult}

import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName}
import com.socrata.soql.typechecker.HasDoc

trait TestHelper {
  implicit val hasType = TestTypeInfo.hasType
  def t(n: Int) = AutoTableLabel.forTest(s"t$n")
  def c(n: Int) = AutoColumnLabel.forTest(s"c$n")
  def rn(n: String) = ResourceName(n)
  def cn(n: String) = ColumnName(n)
  def hn(n: String) = HoleName(n)
  def dcn(n: String) = DatabaseColumnName(n)
  def dtn(n: String) = DatabaseTableName(n)

  def xtest(s: String)(f: => Any): Unit = {}

  class IsomorphicToMatcher[RNS, CT, CV : HasDoc](right: Statement[RNS, CT, CV]) extends BeMatcher[Statement[RNS, CT, CV]] {
    def apply(left: Statement[RNS, CT, CV]) =
      MatchResult(
        left.isIsomorphic(right),
        left.debugStr + "\nwas not isomorphic to\n" + right.debugStr,
        left.debugStr + "\nwas isomorphic to\n" + right.debugStr
      )
  }

  def isomorphicTo[RNS, CT, CV : HasDoc](right: Statement[RNS, CT, CV]) = new IsomorphicToMatcher(right)
}

