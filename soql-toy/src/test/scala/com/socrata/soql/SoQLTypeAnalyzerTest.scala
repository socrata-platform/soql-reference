package com.socrata.soql

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.parsing.Parser
import com.socrata.soql.typechecker.Typechecker
import com.socrata.soql.types._
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}


class SoQLTypeAnalyzerTest extends FunSuite with MustMatchers with ScalaCheckPropertyChecks {
  val datasetCtx = new DatasetContext[SoQLType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> SoQLNumber,
      ColumnName(":updated_at") -> SoQLFixedTimestamp,
      ColumnName(":created_at") -> SoQLFixedTimestamp,
      ColumnName("name_last") -> SoQLText,
      ColumnName("name_first") -> SoQLText,
      ColumnName("visits") -> SoQLNumber,
      ColumnName("last_visit") -> SoQLFixedTimestamp,
      ColumnName("address") -> SoQLLocation,
      ColumnName("balance") -> SoQLMoney,
      ColumnName("object") -> SoQLObject,
      ColumnName("array") -> SoQLArray
    )
  }

  val joinCtx = new DatasetContext[SoQLType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> SoQLNumber,
      ColumnName(":updated_at") -> SoQLFixedTimestamp,
      ColumnName(":created_at") -> SoQLFixedTimestamp,
      ColumnName("name_last") -> SoQLText
    )
  }

  val joinAliasCtx = new DatasetContext[SoQLType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> SoQLNumber,
      ColumnName(":updated_at") -> SoQLFixedTimestamp,
      ColumnName(":created_at") -> SoQLFixedTimestamp,
      ColumnName("name_first") -> SoQLText
    )
  }

  val joinAliasWoOverlapCtx = new DatasetContext[SoQLType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> SoQLNumber,
      ColumnName(":updated_at") -> SoQLFixedTimestamp,
      ColumnName(":created_at") -> SoQLFixedTimestamp,
      ColumnName("x") -> SoQLText,
      ColumnName("y") -> SoQLText,
      ColumnName("z") -> SoQLText
    )
  }

  implicit val datasetCtxMap =
    Map(TableName.PrimaryTable.qualifier -> datasetCtx,
      TableName("_aaaa-aaaa", None).qualifier -> joinCtx,
      TableName("_aaaa-aaab", Some("_a1")).qualifier -> joinAliasCtx,
      TableName("_aaaa-aaax", Some("_x1")).qualifier -> joinAliasWoOverlapCtx,
      TableName("_aaaa-aaab", None).qualifier -> joinAliasCtx,
      TableName("_aaaa-aaax", None).qualifier -> joinAliasWoOverlapCtx)

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  def expression(s: String) = new Parser().expression(s)

  def typedExpression(s: String) = {
    val tc = new Typechecker(SoQLTypeInfo, SoQLFunctionInfo)
    tc(expression(s), Map.empty, None)
  }

  /**
    * This soql once caused excessive number of parser instantiated and
    * exhausted memory due to combinatorial explosion of implicit conversion
    * of datatypes of the concatenation operator.
    *
    * This test could have been placed in SoQLAnalyzerTest if it uses SoQLType instead of TestType which lacks
    * the completeness of SoQLType features to reproduce a combinatorial effect.
    */
  test("typecheck does not excessively expand") {
    val soql = """
      SELECT name_last
       WHERE name_last LIKE '%' || 'Candidate' || '%'
         AND name_last LIKE lower('%' || 'Cohen/Pt. Ruston' || '%')
         AND name_last LIKE lower('%' || 'Tacoma' || '%')
         AND name_last LIKE lower('%' || 'Pt. Ruston' || '%')
         AND name_last LIKE lower('%' || 'Developer' || '%')
      """

    val start = System.currentTimeMillis()
    val analysis = analyzer.analyzeUnchainedQuery(soql)
    val elapsed = (System.currentTimeMillis() - start) / 1000
    analysis.where must not be empty

    // Really this should finish in milliseconds but build servers are
    // slow.  Slow enough that 2s isn't enough to reliably complete!
    elapsed must be < 10L
  }
}
