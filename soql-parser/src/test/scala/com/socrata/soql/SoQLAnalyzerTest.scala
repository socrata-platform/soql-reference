package com.socrata.soql

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers

import com.socrata.soql.parsing.Parser
import com.socrata.soql.types._
import com.socrata.soql.names.ColumnName
import com.socrata.soql.typechecker.Typechecker

class SoQLAnalyzerTest extends FunSuite with MustMatchers {
  implicit val datasetCtx = new DatasetContext[SoQLType] {
    private implicit def ctx = this
    val locale = com.ibm.icu.util.ULocale.ENGLISH
    val schema = com.socrata.collection.OrderedMap(
      ColumnName(":id") -> SoQLNumber,
      ColumnName(":updated_at") -> SoQLFixedTimestamp,
      ColumnName(":created_at") -> SoQLFixedTimestamp,
      ColumnName("name_last") -> SoQLText,
      ColumnName("name_first") -> SoQLText,
      ColumnName("visits") -> SoQLNumber,
      ColumnName("last_visit") -> SoQLFixedTimestamp,
      ColumnName("address") -> SoQLLocation,
      ColumnName("balance") -> SoQLMoney
    )
  }

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo)

  def expression(s: String) = new Parser().expression(s)

  def typedExpression(s: String) = {
    val tc = new Typechecker(SoQLTypeInfo)
    tc(expression(s), Map.empty)
  }

  test("analysis succeeds in a most minimal query") {
    val analysis = analyzer.analyzeFullQuery("select :id")
    analysis.selection.toSeq must equal (Seq(ColumnName(":id") -> typedExpression(":id")))
    analysis.where must be (None)
    analysis.groupBy must be (None)
    analysis.having must be (None)
    analysis.orderBy must be (None)
    analysis.limit must be (None)
    analysis.offset must be (None)
    analysis.isGrouped must be (false)
  }

  test("analysis succeeds in a maximal query") {
    val analysis = analyzer.analyzeFullQuery("select :id as i, sum(balance) where visits > 0 group by i having sum_balance < 5 order by i desc, sum(balance) null first limit 5 offset 10")
    analysis.selection.toSeq must equal (Seq(ColumnName("i") -> typedExpression(":id"), ColumnName("sum_balance") -> typedExpression("sum(balance)")))
    analysis.selection(ColumnName("i")).position.column must equal (8)
    analysis.selection(ColumnName("sum_balance")).position.column must equal (18)
    analysis.where must equal (Some(typedExpression("visits > 0")))
    analysis.where.get.position.column must equal (37)
    analysis.where.get.asInstanceOf[typed.FunctionCall[_]].functionNamePosition.column must equal (44)
    analysis.groupBy must equal (Some(Seq(typedExpression(":id"))))
    analysis.groupBy.get(0).position.column must equal (8)
    analysis.having must equal (Some(typedExpression("sum(balance) < 5")))
    analysis.having.get.position.column must equal (66)
    analysis.having.get.asInstanceOf[typed.FunctionCall[_]].parameters(0).position.column must equal (18)
    analysis.having.get.asInstanceOf[typed.FunctionCall[_]].functionNamePosition.column must equal (78)
    analysis.orderBy must equal (Some(Seq(typed.OrderBy(typedExpression(":id"), false, true), typed.OrderBy(typedExpression("sum(balance)"), true, false))))
    analysis.orderBy.get.map(_.expression.position.column) must equal (Seq(8, 99))
    analysis.limit must equal (Some(BigInt(5)))
    analysis.offset must equal (Some(BigInt(10)))
  }

  test("Giving no values to the split-query analyzer returns the equivalent of `SELECT *'") {
    val analysis = analyzer.analyzeSplitQuery(None, None, None, None, None, None, None)
    analysis must equal (analyzer.analyzeFullQuery("SELECT *"))
  }

  test("Putting an aggregate in the order-by slot causes aggregation to occur") {
    val analysis = analyzer.analyzeSplitQuery(None, None, None, None, Some("max(visits)"), None, None)
    analysis must equal (analyzer.analyzeFullQuery("SELECT count(*) ORDER BY max(visits)"))
  }

  test("Having a group by clause puts them in the selection list") {
    val computed = "name_first || ' ' || name_last"
    val sep = ", "
    val uncomputed = "visits"
    val analysis = analyzer.analyzeSplitQuery(None, None, Some(computed + sep + uncomputed), None, None, None, None)
    analysis must equal (analyzer.analyzeFullQuery("SELECT "+computed+", "+uncomputed+", count(*) GROUP BY "+computed+", "+uncomputed))
    analysis.selection(ColumnName("visits")).position.column must equal (1 + computed.length + sep.length)
  }
}
