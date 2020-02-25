package com.socrata.soql

import scala.util.parsing.input.NoPosition
import org.scalatest.prop.PropertyChecks
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import com.socrata.soql.environment.{ColumnName, DatasetContext, ResourceName, Qualified, TableRef}
import com.socrata.soql.parsing.Parser
import com.socrata.soql.typechecker.Typechecker
import com.socrata.soql.types._
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}


class SoQLTypeAnalyzerTest extends FunSuite with MustMatchers with PropertyChecks {
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

  val primary = ResourceName("primary")

  implicit val datasetCtxMap =
    Map(primary -> datasetCtx,
      ResourceName("aaaa-aaaa") -> joinCtx,
      ResourceName("aaaa-aaab") -> joinAliasCtx,
      ResourceName("aaaa-aaax") -> joinAliasWoOverlapCtx)

  def tableFinder(in: Set[ResourceName]) = datasetCtxMap.filterKeys(in)

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo, tableFinder)
  type TypedExpr = typed.CoreExpr[Qualified[ColumnName], SoQLType]

  def expression(s: String) = new Parser().expression(s)

  def typedExpression(s: String): TypedExpr = {
    typedExpression(s, Map.empty[ResourceName, (TableRef, DatasetContext[SoQLType])])
  }

  def typedExpression(s: String, otherCtxs: Map[ResourceName, (TableRef, Map[ColumnName, SoQLType])]): TypedExpr = {
    val tc = new Typechecker(SoQLTypeInfo, SoQLFunctionInfo)
    def convertTypes(ref: TableRef, columnName: ColumnName, columnType: SoQLType): TypedExpr = {
      typed.ColumnRef[Qualified[ColumnName], SoQLType](
        Qualified(ref, columnName),
        columnType)(NoPosition)
    }
    tc(expression(s), Typechecker.Ctx(datasetCtx.schema.transform(convertTypes(TableRef.Primary, _ ,_)),
                                      otherCtxs.mapValues { case (ref, schema) =>
                                        schema.transform(convertTypes(ref, _, _))
                                      }))
  }
  def typedExpression(s: String, otherCtxs: Map[ResourceName, (TableRef, DatasetContext[SoQLType])])(implicit erasureEvasion: Unit = ()): TypedExpr = {
    typedExpression(s, otherCtxs.mapValues { case (ref, dc) => (ref, dc.schema) })
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
    val analysis = analyzer.analyzeUnchainedQuery(primary, soql)
    val elapsed = (System.currentTimeMillis() - start) / 1000
    analysis.where must not be empty

    // Really this should finish in milliseconds but build servers are
    // slow.  Slow enough that 2s isn't enough to reliably complete!
    elapsed must be < 10L
  }
}
