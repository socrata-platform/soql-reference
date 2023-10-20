package com.socrata.soql.analyzer2

import org.scalatest.Assertions
import org.scalatest.matchers.{BeMatcher, MatchResult}

import com.socrata.soql.ast
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName, Provenance}

import mocktablefinder._

object TestHelper {
  final class TestMT extends MetaTypes {
    type ResourceNameScope = Int
    type ColumnType = TestType
    type ColumnValue = TestValue
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
  }

  val testTypeInfoProjection = TestTypeInfo.metaProject[TestMT]

  object TestProvenanceMapper extends types.ProvenanceMapper[TestMT] {
    def fromProvenance(prov: Provenance) = DatabaseTableName(prov.value)
    def toProvenance(dtn: types.DatabaseTableName[TestMT]) = Provenance(dtn.name)
  }
}

trait TestHelper { this: Assertions =>
  type TestMT = TestHelper.TestMT

  implicit val hasType = TestHelper.testTypeInfoProjection.hasType
  def t(n: Int) = AutoTableLabel.forTest(n)
  def c(n: Int) = AutoColumnLabel.forTest(n)
  def rn(n: String) = ResourceName(n)
  def cn(n: String) = ColumnName(n)
  def hn(n: String) = HoleName(n)
  def dcn(n: String) = DatabaseColumnName(n)
  def dtn(n: String) = DatabaseTableName(n)
  def canon(n: String) = CanonicalName(n)

  def xtest(s: String)(f: => Any): Unit = {}

  def tableFinder(items: ((Int, String), Thing[Int, TestType])*) = new MockTableFinder[TestMT](items.toMap)

  def isLiteralTrue(e: Expr[TestMT]) =
    e match {
      case LiteralValue(TestBoolean(true)) => true
      case _ => false
    }

  val analyzer = new SoQLAnalyzer[TestMT](TestTypeInfo, TestFunctionInfo, TestHelper.TestProvenanceMapper)
  val systemColumnPreservingAnalyzer = analyzer.preserveSystemColumns { (_, expr) =>
    expr.typ match {
      case TestNumber =>
        Some(
          AggregateFunctionCall[TestMT](
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

  class IsomorphicToMatcher[MT <: MetaTypes](right: Statement[MT])(implicit ev: HasDoc[MT#ColumnValue], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]) extends BeMatcher[Statement[MT]] {
    def apply(left: Statement[MT]) =
      MatchResult(
        left.isIsomorphic(right),
        left.debugStr + "\nwas not isomorphic to\n" + right.debugStr,
        left.debugStr + "\nwas isomorphic to\n" + right.debugStr
      )
  }

  def isomorphicTo[MT <: MetaTypes](right: Statement[MT])(implicit ev: HasDoc[MT#ColumnValue], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]) = new IsomorphicToMatcher(right)

  class VerticalSliceMatcher[MT <: MetaTypes](right: Statement[MT])(implicit ev: HasDoc[MT#ColumnValue], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]) extends BeMatcher[Statement[MT]] {
    def apply(left: Statement[MT]) =
      MatchResult(
        left.isVerticalSlice(right),
        left.debugStr + "\nwas not a vertical slice of\n" + right.debugStr,
        left.debugStr + "\nwas a vertical slice of\n" + right.debugStr
      )
  }

  def verticalSliceOf[MT <: MetaTypes](right: Statement[MT])(implicit ev: HasDoc[MT#ColumnValue], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]) = new VerticalSliceMatcher(right)

  def specFor(params: Map[HoleName, UserParameters.PossibleValue[TestType, TestValue]]): Map[HoleName, TestType] =
    params.iterator.map { case (hn, cv) =>
      val typ = cv match {
        case UserParameters.Null(t) => t
        case UserParameters.Value(v) => hasType.typeOf(v)
      }
      hn -> typ
    }.toMap

  type TF = TableFinder[TestMT]

  class OnFail {
    def onAnalyzerError(err: SoQLAnalyzerError[Int]): Nothing =
      fail(err.toString)

    def onTableFinderError(err: TableFinderError[Int]): Nothing =
      fail(err.toString)
  }

  implicit val DefaultOnFail = new OnFail

  object AnalysisBuilder {
    def apply(
      foundTables: FoundTables[TestMT],
      params: UserParameters[TestType, TestValue],
      onFail: OnFail
    ): AnalysisBuilder =
      new AnalysisBuilder(foundTables, params, onFail, false)

    def apply(tf: TF, params: UserParameters[TestType, TestValue], onFail: OnFail)(f: TF => Either[TableFinderError[Int], FoundTables[TestMT]]): AnalysisBuilder =
      f(tf) match {
        case Right(ft) =>
          apply(ft, params, onFail)
        case Left(err) =>
          onFail.onTableFinderError(err)
      }

    def saved(tf: TF, ctx: String)(implicit onFail: OnFail): AnalysisBuilder =
      saved(tf, ctx, UserParameters.empty)(onFail)

    def saved(tf: TF, ctx: String, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): AnalysisBuilder =
      apply(tf, params, onFail)(_.findTables(0, rn(ctx)))

    def analyze(tf: TF, ctx: String, query: String)(implicit onFail: OnFail): AnalysisBuilder =
      analyze(tf, ctx, query, UserParameters.empty)(onFail)

    def analyze(tf: TF, ctx: String, query: String, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): AnalysisBuilder =
      apply(tf, params, onFail)(_.findTables(0, rn(ctx), query, specFor(params.unqualified)))

    def analyze(tf: TF, ctx: String, query: String, canonicalName: CanonicalName, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): AnalysisBuilder = {
      AnalysisBuilder(tf, params, onFail)(_.findTables(0, rn(ctx), query, Map.empty, canonicalName))
    }

    def analyze(tf: TF, query: String)(implicit onFail: OnFail): AnalysisBuilder =
      analyze(tf, query, UserParameters.empty)(onFail)

    def analyze(tf: TF, query: String, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): AnalysisBuilder =
      apply(tf, params, onFail)(_.findTables(0, query, specFor(params.unqualified)))
  }

  class AnalysisBuilder(
    foundTables: FoundTables[TestMT],
    params: UserParameters[TestType, TestValue],
    onFail: OnFail,
    preserveSystemColumns: Boolean
  ) {
    private def effectiveAnalyzer =
      if(preserveSystemColumns) analyzer.preserveSystemColumns(aggregateMerge)
      else analyzer

    def withPreserveSystemColumns(psc: Boolean): AnalysisBuilder =
      new AnalysisBuilder(foundTables, params, onFail, psc)

    def finishAnalysis: SoQLAnalysis[TestMT] = {
      effectiveAnalyzer(foundTables, params) match {
        case Right(result) => result
        case Left(err) => onFail.onAnalyzerError(err)
      }
    }
  }

  private def aggregateMerge(colName: ColumnName, expr: Expr[TestMT]): Option[Expr[TestMT]] = {
    if(colName == ColumnName(":id")) {
      Some(AggregateFunctionCall[TestMT](TestFunctions.Max.monomorphic.get, Seq(expr), false, None)(FuncallPositionInfo.None))
    } else {
      None
    }
  }

  def analyzeSaved(tf: TF, ctx: String)(implicit onFail: OnFail): SoQLAnalysis[TestMT] = {
    AnalysisBuilder.saved(tf, ctx)(onFail).finishAnalysis
  }

  def analyzeSaved(tf: TF, ctx: String, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): SoQLAnalysis[TestMT] = {
    AnalysisBuilder.saved(tf, ctx, params)(onFail).finishAnalysis
  }

  def analyze(tf: TF, ctx: String, query: String)(implicit onFail: OnFail): SoQLAnalysis[TestMT] = {
    AnalysisBuilder.analyze(tf, ctx, query, UserParameters.empty)(onFail).finishAnalysis
  }

  def analyze(tf: TF, ctx: String, query: String, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): SoQLAnalysis[TestMT] = {
    AnalysisBuilder.analyze(tf, ctx, query, params)(onFail).finishAnalysis
  }

  def analyze(tf: TF, ctx: String, query: String, canonicalName: CanonicalName, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): SoQLAnalysis[TestMT] = {
    AnalysisBuilder.analyze(tf, ctx, query, canonicalName, params)(onFail).
      finishAnalysis
  }

  def analyze(tf: TF, query: String)(implicit onFail: OnFail): SoQLAnalysis[TestMT] = {
    AnalysisBuilder.analyze(tf, query, UserParameters.empty)(onFail).
      finishAnalysis
  }

  def analyze(tf: TF, query: String, params: UserParameters[TestType, TestValue])(implicit onFail: OnFail): SoQLAnalysis[TestMT] = {
    AnalysisBuilder.analyze(tf, query, params)(onFail).
      finishAnalysis
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

  implicit object TestSoQLApproximationMetaOps extends SoQLApproximation.MetaOps[TestMT] {
    def databaseTableName(dtn: DatabaseTableName): String = {
      dtn.name
    }

    def databaseColumnName(dcn: DatabaseColumnName): ColumnName = {
      ColumnName(dcn.name)
    }

    def literalValue(value: LiteralValue): ast.Expression =
      value.value match {
        case TestText(s) => ast.StringLiteral(s)(value.position.logicalPosition)
        case TestNumber(n) => ast.NumberLiteral(n)(value.position.logicalPosition)
        case TestBoolean(b) => ast.BooleanLiteral(b)(value.position.logicalPosition)
        case TestUnorderable(s) => ast.StringLiteral(s)(value.position.logicalPosition)
        case TestNull => ast.NullLiteral()(value.position.logicalPosition)
      }

    override def auto_table_label_prefix = "t"
    override def auto_column_label_prefix = "c"
  }
}

