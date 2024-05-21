package com.socrata.soql.stdlib.analyzer2.rollup

import java.math.{BigDecimal => JBigDecimal}

import org.scalatest.{FunSuite, MustMatchers}
import org.scalatest.matchers.{BeMatcher, MatchResult}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.analyzer2.rollup.RollupInfo
import com.socrata.soql.environment.{Provenance, ResourceName, ScopedResourceName}
import com.socrata.soql.functions.{SoQLFunctions, MonomorphicFunction, SoQLTypeInfo, SoQLFunctionInfo}
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider

object SoQLRollupTestHelper {
  trait MT extends MetaTypes {
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type ResourceNameScope = Int
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
  }
}

trait SoQLRollupTestHelper extends FunSuite with MustMatchers with StatementUniverse[SoQLRollupTestHelper.MT] {
  import SoQLTypeInfo.hasType

  type MT = SoQLRollupTestHelper.MT

  implicit object cvDoc extends HasDoc[CV] {
    def docOf(cv: CV) = cv.doc(CryptProvider.zeros)
  }

  def xtest[T](s: String)(f: => T): Unit = {}

  class IsomorphicToMatcher(right: Statement) extends BeMatcher[Statement] {
    def apply(left: Statement) =
      MatchResult(
        left.isIsomorphic(right),
        left.debugStr + "\nwas not isomorphic to\n" + right.debugStr,
        left.debugStr + "\nwas isomorphic to\n" + right.debugStr
      )
  }

  def isomorphicTo(right: Statement) = new IsomorphicToMatcher(right)

  val analyzer = new SoQLAnalyzer[MT](
    SoQLTypeInfo.soqlTypeInfo2,
    SoQLFunctionInfo,
    new ToProvenance {
      override def toProvenance(dtn: DatabaseTableName) = Provenance(dtn.name)
    })

  val rollupExact = new SoQLRollupExact[MT](Stringifier.simple)

  class SoQLRollupInfo(
    val id: Int,
    val statement: Statement,
    val resourceName: types.ScopedResourceName[MT],
    val databaseName: types.DatabaseTableName[MT]
  ) extends RollupInfo[MT, Int] {
    override def databaseColumnNameOfIndex(i: Int) = DatabaseColumnName(s"c${i+1}")
  }

  object SoQLRollupInfo {
    def apply(id: Int, name: String, tf: TableFinder[MT], soql: String): SoQLRollupInfo = {
      val Right(foundTables) = tf.findTables(0, soql, Map.empty)
      val analysis = analyzer(foundTables, UserParameters.empty) match {
        case Right(a) => a
        case Left(e) => fail(e.toString)
      }
      new SoQLRollupInfo(id, analysis.statement, ScopedResourceName(0, ResourceName(name)), DatabaseTableName(name))
    }
  }

  def analyze(tf: TableFinder[MT], q: String) = {
    val Right(ft) = tf.findTables(0, q, Map.empty)
    val Right(analysis) = analyzer(ft, UserParameters.empty)
    analysis
  }
}
