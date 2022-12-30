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
  val systemColumnPreservingAnalyzer = analyzer.preserveSystemColumns { (_, expr) =>
    expr.typ match {
      case TestNumber =>
        Some(
          AggregateFunctionCall(
            TestFunctions.Max.monomorphic.get,
            Seq(expr),
            false,
            None
          )(FuncallPositionInfo.None)
        )
      case _ =>
        None
    }
  }

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

  class OnFail {
    def onAnalyzerError(err: SoQLAnalyzerError[Int, SoQLAnalyzerError.AnalysisError]): Nothing =
      fail(err.toString)

    def onTableFinderError(err: SoQLAnalyzerError[Int, SoQLAnalyzerError.TableFinderError]): Nothing =
      fail(err.toString)
  }

  implicit val DefaultOnFail = new OnFail

  private def finishAnalysis(start: FoundTables[Int, TestType], params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): SoQLAnalysis[Int, TestType, TestValue] = {
    analyzer(start, params) match {
      case Right(result) => result
      case Left(err) => onFail.onAnalyzerError(err)
    }
  }

  def analyzeSaved(tf: TF[TestType], ctx: String)(implicit onFail: OnFail): SoQLAnalysis[Int, TestType, TestValue] = {
    analyzeSaved(tf, ctx, UserParameters.empty)(onFail)
  }

  def analyzeSaved(tf: TF[TestType], ctx: String, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): SoQLAnalysis[Int, TestType, TestValue] = {
    tf.findTables(0, rn(ctx)) match {
      case Right(start) =>
        finishAnalysis(start, params)(onFail)
      case Left(e) =>
        onFail.onTableFinderError(e)
    }
  }

  def analyze(tf: TF[TestType], ctx: String, query: String)(implicit onFail: OnFail): SoQLAnalysis[Int, TestType, TestValue] = {
    analyze(tf, ctx, query, UserParameters.empty)(onFail)
  }

  def analyze(tf: TF[TestType], ctx: String, query: String, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): SoQLAnalysis[Int, TestType, TestValue] = {
    tf.findTables(0, rn(ctx), query, specFor(params.unqualified)) match {
      case Right(start) =>
        finishAnalysis(start, params)(onFail)
      case Left(e) =>
        onFail.onTableFinderError(e)
    }
  }

  def analyze(tf: TF[TestType], ctx: String, query: String, canonicalName: CanonicalName, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): SoQLAnalysis[Int, TestType, TestValue] = {
    tf.findTables(0, rn(ctx), query, Map.empty, canonicalName) match {
      case Right(start) =>
        finishAnalysis(start, params)(onFail)
      case Left(e) =>
        onFail.onTableFinderError(e)
    }
  }

  def analyze(tf: TF[TestType], query: String)(implicit onFail: OnFail): SoQLAnalysis[Int, TestType, TestValue] = {
    analyze(tf, query, UserParameters.empty)(onFail)
  }

  def analyze(tf: TF[TestType], query: String, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): SoQLAnalysis[Int, TestType, TestValue] = {
    tf.findTables(0, query, specFor(params.unqualified)) match {
      case Right(start) =>
        finishAnalysis(start, params)(onFail)
      case Left(e) =>
        onFail.onTableFinderError(e)
    }
  }

  case class ExpectedFailure(ident: Any) extends Throwable
  def expected(ident: Any) = throw ExpectedFailure(ident)

  def expectFailure[T](ident: Any)(f: => T): Unit = {
    try {
      f
      fail(s"Expected failure $ident but it did not happen")
    } catch {
      case ExpectedFailure(`ident`) =>
        // yay
    }
  }
}

