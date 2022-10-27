package com.socrata.soql.analyzer2

import org.scalatest.Assertions
import org.scalatest.matchers.{BeMatcher, MatchResult}

import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName}
import com.socrata.soql.typechecker.HasDoc

import mocktablefinder._

trait TestHelper { this: Assertions =>
  implicit val hasType = TestTypeInfo.hasType
  def t(n: Int) = AutoTableLabel.forTest(n)
  def c(n: Int) = AutoColumnLabel.forTest(n)
  def rn(n: String) = ResourceName(n)
  def cn(n: String) = ColumnName(n)
  def hn(n: String) = HoleName(n)
  def dcn(n: String) = DatabaseColumnName(n)
  def dtn(n: String) = DatabaseTableName(n)

  def xtest(s: String)(f: => Any): Unit = {}

  def tableFinder[RNS](items: ((RNS, String), Thing[RNS, TestType])*) = new MockTableFinder[RNS, TestType](items.toMap)

  val analyzer = new SoQLAnalyzer[Int, TestType, TestValue](TestTypeInfo, TestFunctionInfo)

  class IsomorphicToMatcher[RNS, CT, CV : HasDoc](right: Statement[RNS, CT, CV]) extends BeMatcher[Statement[RNS, CT, CV]] {
    def apply(left: Statement[RNS, CT, CV]) =
      MatchResult(
        left.isIsomorphic(right),
        left.debugStr + "\nwas not isomorphic to\n" + right.debugStr,
        left.debugStr + "\nwas isomorphic to\n" + right.debugStr
      )
  }

  def isomorphicTo[RNS, CT, CV : HasDoc](right: Statement[RNS, CT, CV]) = new IsomorphicToMatcher(right)

  def specFor(params: Map[HoleName, UserParameters.PossibleValue[TestType, TestValue]]): Map[HoleName, TestType] =
    params.iterator.map { case (hn, cv) =>
      val typ = cv match {
        case UserParameters.Null(t) => t
        case UserParameters.Value(v) => hasType.typeOf(v)
      }
      hn -> typ
    }.toMap

  type TF[CT] = TableFinder {
    type ResourceNameScope = Int
    type ColumnType = CT
  }

  private def finishAnalysis(start: FoundTables[Int, TestType], params: UserParameters[TestType, TestValue]): SoQLAnalysis[Int, TestType, TestValue] = {
    analyzer(start, params) match {
      case Right(result) => result
      case Left(err) => fail(err.toString)
    }
  }

  def analyzeSaved(tf: TF[TestType], ctx: String): SoQLAnalysis[Int, TestType, TestValue] = {
    analyzeSaved(tf, ctx, UserParameters.empty)
  }

  def analyzeSaved(tf: TF[TestType], ctx: String, params: UserParameters[TestType, TestValue]): SoQLAnalysis[Int, TestType, TestValue] = {
    tf.findTables(0, rn(ctx)) match {
      case tf.Success(start) =>
        finishAnalysis(start, params)
      case e: tf.Error =>
        fail(e.toString)
    }
  }

  def analyze(tf: TF[TestType], ctx: String, query: String): SoQLAnalysis[Int, TestType, TestValue] = {
    analyze(tf, ctx, query, UserParameters.empty)
  }

  def analyze(tf: TF[TestType], ctx: String, query: String, params: UserParameters[TestType, TestValue]): SoQLAnalysis[Int, TestType, TestValue] = {
    tf.findTables(0, rn(ctx), query, specFor(params.unqualified)) match {
      case tf.Success(start) =>
        finishAnalysis(start, params)
      case e: tf.Error =>
        fail(e.toString)
    }
  }

  def analyze(tf: TF[TestType], ctx: String, query: String, canonicalName: CanonicalName, params: UserParameters[TestType, TestValue]): SoQLAnalysis[Int, TestType, TestValue] = {
    tf.findTables(0, rn(ctx), query, Map.empty, canonicalName) match {
      case tf.Success(start) =>
        finishAnalysis(start, params)
      case e: tf.Error =>
        fail(e.toString)
    }
  }

  def analyze(tf: TF[TestType], query: String): SoQLAnalysis[Int, TestType, TestValue] = {
    analyze(tf, query, UserParameters.empty)
  }

  def analyze(tf: TF[TestType], query: String, params: UserParameters[TestType, TestValue]): SoQLAnalysis[Int, TestType, TestValue] = {
    tf.findTables(0, query, specFor(params.unqualified)) match {
      case tf.Success(start) =>
        finishAnalysis(start, params)
      case e: tf.Error =>
        fail(e.toString)
    }
  }
}

