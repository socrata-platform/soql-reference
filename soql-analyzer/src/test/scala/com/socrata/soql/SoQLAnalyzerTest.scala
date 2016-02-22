package com.socrata.soql

import com.socrata.soql.exceptions.{NonGroupableGroupBy, NonBooleanHaving, NonBooleanWhere, UnorderableOrderBy}
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.prop.PropertyChecks

import scala.util.parsing.input.NoPosition

import org.scalatest.FunSuite
import org.scalatest.MustMatchers

import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.parsing.Parser
import com.socrata.soql.typechecker.Typechecker
import com.socrata.soql.types._
import com.socrata.soql.functions.MonomorphicFunction

class SoQLAnalyzerTest extends FunSuite with MustMatchers with PropertyChecks {
  implicit val datasetCtx = new DatasetContext[TestType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> TestNumber,
      ColumnName(":updated_at") -> TestFixedTimestamp,
      ColumnName(":created_at") -> TestFixedTimestamp,
      ColumnName("name_last") -> TestText,
      ColumnName("name_first") -> TestText,
      ColumnName("visits") -> TestNumber,
      ColumnName("last_visit") -> TestFixedTimestamp,
      ColumnName("address") -> TestLocation,
      ColumnName("balance") -> TestMoney,
      ColumnName("object") -> TestObject,
      ColumnName("array") -> TestArray
    )
  }

  val analyzer = new SoQLAnalyzer(TestTypeInfo, TestFunctionInfo)

  def expression(s: String) = new Parser().expression(s)

  def typedExpression(s: String) = {
    val tc = new Typechecker(TestTypeInfo, TestFunctionInfo)
    tc(expression(s), Map.empty)
  }

  test("analysis succeeds in a most minimal query") {
    val analysis = analyzer.analyzeUnchainedQuery("select :id")
    analysis.selection.toSeq must equal (Seq(ColumnName(":id") -> typedExpression(":id")))
    analysis.where must be (None)
    analysis.groupBy must be (None)
    analysis.having must be (None)
    analysis.orderBy must be (None)
    analysis.limit must be (None)
    analysis.offset must be (None)
    analysis.isGrouped must be (false)
  }

  test("analysis succeeds in a maximal group-by query") {
    val analysis = analyzer.analyzeUnchainedQuery("select :id as i, sum(balance) where visits > 0 group by i having sum_balance < 5 order by i desc null last, sum(balance) null first limit 5 offset 10")
    analysis.selection.toSeq must equal (Seq(ColumnName("i") -> typedExpression(":id"), ColumnName("sum_balance") -> typedExpression("sum(balance)")))
    analysis.selection(ColumnName("i")).position.column must equal (8)
    analysis.selection(ColumnName("sum_balance")).position.column must equal (18)
    analysis.where must equal (Some(typedExpression("visits > 0")))
    analysis.where.get.position.column must equal (37)
    analysis.where.get.asInstanceOf[typed.FunctionCall[_,_]].functionNamePosition.column must equal (44)
    analysis.groupBy must equal (Some(Seq(typedExpression(":id"))))
    analysis.groupBy.get(0).position.column must equal (8)
    analysis.having must equal (Some(typedExpression("sum(balance) < 5")))
    analysis.having.get.position.column must equal (66)
    analysis.having.get.asInstanceOf[typed.FunctionCall[_,_]].parameters(0).position.column must equal (18)
    analysis.having.get.asInstanceOf[typed.FunctionCall[_,_]].functionNamePosition.column must equal (78)
    analysis.orderBy must equal (Some(Seq(typed.OrderBy(typedExpression(":id"), false, true), typed.OrderBy(typedExpression("sum(balance)"), true, false))))
    analysis.orderBy.get.map(_.expression.position.column) must equal (Seq(8, 109))
    analysis.limit must equal (Some(BigInt(5)))
    analysis.offset must equal (Some(BigInt(10)))
  }

  test("analysis succeeds in a maximal ungrouped query") {
    val analysis = analyzer.analyzeUnchainedQuery("select :*, *(except name_first, name_last), nf || (' ' || nl) as name, name_first as nf, name_last as nl where nl < 'm' order by name desc, visits limit 5 offset 10")
    analysis.selection.toSeq must equal (datasetCtx.schema.toSeq.filterNot(_._1.name.startsWith("name_")).map { case (n, t) => n -> typed.ColumnRef(n, t)(NoPosition) } ++ Seq(ColumnName("name") -> typedExpression("name_first || (' ' || name_last)"), ColumnName("nf") -> typedExpression("name_first"), ColumnName("nl") -> typedExpression("name_last")))
    analysis.selection(ColumnName(":id")).position.column must equal (8)
    analysis.selection(ColumnName(":updated_at")).position.column must equal (8)
    analysis.selection(ColumnName(":created_at")).position.column must equal (8)
    analysis.selection(ColumnName("visits")).position.column must equal (12)
    analysis.selection(ColumnName("last_visit")).position.column must equal (12)
    analysis.selection(ColumnName("address")).position.column must equal (12)
    analysis.selection(ColumnName("balance")).position.column must equal (12)
    analysis.selection(ColumnName("name")).position.column must equal (45)
    analysis.selection(ColumnName("nf")).position.column must equal (72)
    analysis.selection(ColumnName("nl")).position.column must equal (90)
    analysis.where must equal (Some(typedExpression("name_last < 'm'")))
    analysis.where.get.position.column must equal (112)
    analysis.where.get.asInstanceOf[typed.FunctionCall[_,_]].functionNamePosition.column must equal (115)
    analysis.where.get.asInstanceOf[typed.FunctionCall[_,_]].parameters(0).position.column must equal (90)
    analysis.groupBy must equal (None)
    analysis.having must equal (None)
    analysis.orderBy must equal (Some(Seq(typed.OrderBy(typedExpression("name_first || (' ' || name_last)"), false, false), typed.OrderBy(typedExpression("visits"), true, true))))
    analysis.orderBy.get.map(_.expression.position.column) must equal (Seq(45, 141))
    analysis.limit must equal (Some(BigInt(5)))
    analysis.offset must equal (Some(BigInt(10)))
  }

  test("Giving no values to the split-query analyzer returns the equivalent of `SELECT *'") {
    val analysis = analyzer.analyzeSplitQuery(None, None, None, None, None, None, None, None)
    analysis must equal (analyzer.analyzeUnchainedQuery("SELECT *"))
  }

  test("Putting an aggregate in the order-by slot causes aggregation to occur") {
    val analysis = analyzer.analyzeSplitQuery(None, None, None, None, Some("max(visits)"), None, None, None)
    analysis must equal (analyzer.analyzeUnchainedQuery("SELECT count(*) ORDER BY max(visits)"))
  }

  test("Having a group by clause puts them in the selection list") {
    val computed = "name_first || ' ' || name_last"
    val sep = ", "
    val uncomputed = "visits"
    val analysis = analyzer.analyzeSplitQuery(None, None, Some(computed + sep + uncomputed), None, None, None, None, None)
    analysis must equal (analyzer.analyzeUnchainedQuery("SELECT "+computed+", "+uncomputed+", count(*) GROUP BY "+computed+", "+uncomputed))
    analysis.selection(ColumnName("visits")).position.column must equal (1 + computed.length + sep.length)
  }

  test("analysis succeeds in cast") {
    val analysis = analyzer.analyzeUnchainedQuery("select name_last::number as c1, '123'::number as c2, 456::text as c3")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("c1") -> typedExpression("name_last::number"),
      ColumnName("c2") -> typedExpression("'123'::number"),
      ColumnName("c3") -> typedExpression("456::text")
    ))
  }

  test("json property and index") {
    val analysis = analyzer.analyzeUnchainedQuery("select object.xxxx as c1, array[123] as c2, object.yyyy::text as c3, array[123]::number as c4")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("c1") -> typedExpression("object.xxxx"),
      ColumnName("c2") -> typedExpression("array[123]"),
      ColumnName("c3") -> typedExpression("object.yyyy::text"),
      ColumnName("c4") -> typedExpression("array[123]::number")
    ))
  }

  test("null :: number succeeds") {
    val analysis = analyzer.analyzeUnchainedQuery("select null :: number as x")
    analysis.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestNumber)).get.monomorphic.get,
      Seq(typed.NullLiteral(TestNumber.t)(NoPosition)))(NoPosition, NoPosition))
  }

  test("5 :: number succeeds") {
    val analysis = analyzer.analyzeUnchainedQuery("select 5 :: number as x")
    analysis.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestNumber)).get.monomorphic.get,
      Seq(typed.NumberLiteral(java.math.BigDecimal.valueOf(5), TestNumber.t)(NoPosition)))(NoPosition, NoPosition))
  }

  test("5 :: money succeeds") {
    val analysis = analyzer.analyzeUnchainedQuery("select 5 :: money as x")
    analysis.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestMoney)).get.monomorphic.get,
      Seq(typed.FunctionCall(TestFunctions.NumberToMoney.monomorphic.get, Seq(typed.NumberLiteral(java.math.BigDecimal.valueOf(5), TestNumber.t)(NoPosition)))(NoPosition, NoPosition)))(NoPosition, NoPosition))
  }

  test("a subselect makes the output of the inner select available to the outer") {
    val Seq(inner, outer) = analyzer.analyzeFullQuery("select 5 :: money as x |> select max(x)")
    outer.selection(ColumnName("max_x")) must equal (typed.FunctionCall(MonomorphicFunction(TestFunctions.Max, Map("a" -> TestMoney)), Seq(typed.ColumnRef(ColumnName("x"), TestMoney : TestType)(NoPosition)))(NoPosition, NoPosition))
    inner.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestMoney)).get.monomorphic.get,
      Seq(typed.FunctionCall(TestFunctions.NumberToMoney.monomorphic.get, Seq(typed.NumberLiteral(java.math.BigDecimal.valueOf(5), TestNumber.t)(NoPosition)))(NoPosition, NoPosition)))(NoPosition, NoPosition))
  }

  test("cannot ORDER BY an unorderable type") {
    an [UnorderableOrderBy] must be thrownBy analyzer.analyzeFullQuery("select * order by array")
  }

  test("cannot filter by a non-boolean type") {
    a [NonBooleanWhere] must be thrownBy analyzer.analyzeFullQuery("select * where array")
    a [NonBooleanHaving] must be thrownBy analyzer.analyzeFullQuery("select count(*) having count(*)")
  }

  test("cannot group by a non-groupable type") {
    a [NonGroupableGroupBy] must be thrownBy analyzer.analyzeFullQuery("select array group by array")
  }

  test("Merging no stages returns no stages") {
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, Nil) must equal (Nil)
  }

  test("Merging two simple filter queries is the same as querying one") {
    val analysis1 = analyzer.analyzeFullQuery("select 2*visits as twice_visits where twice_visits > 10 |> select * where twice_visits > 20")
    val analysis2 = analyzer.analyzeFullQuery("select 2*visits as twice_visits where twice_visits > 10 and twice_visits > 20")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging a filter-on-a-group-on-a-filter is the same as a where-group-having one") {
    val analysis1 = analyzer.analyzeFullQuery("select visits where visits > 10 |> select visits, count(*) as c group by visits |> select * where c > 5")
    val analysis2 = analyzer.analyzeFullQuery("select visits, count(*) as c where visits > 10 group by visits having c > 5")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging limits truncates the second limit to the window defined by the first") {
    val analysis1 = analyzer.analyzeFullQuery("select visits offset 10 limit 5 |> select * offset 3 limit 10")
    val analysis2 = analyzer.analyzeFullQuery("select visits offset 13 limit 2")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging an offset to the end of a limit reduces the limit to 0") {
    val analysis1 = analyzer.analyzeFullQuery("select visits offset 10 limit 5 |> select * offset 5")
    val analysis2 = analyzer.analyzeFullQuery("select visits offset 15 limit 0")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging an offset past the end of a limit reduces the limit to 0 and does not move the offset past the end of the first query") {
    val analysis1 = analyzer.analyzeFullQuery("select visits offset 10 limit 5 |> select * offset 50")
    val analysis2 = analyzer.analyzeFullQuery("select visits offset 15 limit 0")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Limit-combining produces the intersection of the two regions") {
    forAll { (aLim0: Option[BigInt], aOff0: Option[BigInt], bLim0: Option[BigInt], bOff0: Option[BigInt]) =>
      val aLim = aLim0.map(_.abs)
      val aOff = aOff0.map(_.abs)
      val bLim = bLim0.map(_.abs)
      val bOff = bOff0.map(_.abs)

      val (cLim, cOff) = Merger.combineLimits(aLim, aOff, bLim, bOff)

      val trueAOff = aOff.getOrElse(BigInt(0))
      val trueBOff = trueAOff + bOff.getOrElse(BigInt(0))
      val trueCOff = cOff.getOrElse(BigInt(0))

      sealed trait Bound
      case class Value(x: BigInt) extends Bound
      case object Infinite extends Bound
      def asBound(x: Option[BigInt]) = x.fold[Bound](Infinite)(Value)
      implicit object boundOrd extends Ordering[Bound] {
        override def compare(x: Bound, y: Bound): Int = (x, y) match {
          case (Value(a), Value(b)) => a.compare(b)
          case (Infinite, Value(_)) => 1
          case (Value(_), Infinite) => -1
          case (Infinite, Infinite) => 0
        }
      }

      val trueAEnd = asBound(aLim.map(_ + trueAOff))
      val trueBEnd = asBound(bLim.map(_ + trueBOff))
      val trueCEnd = asBound(cLim.map(_ + trueCOff))

      Value(trueCOff) must equal (List(Value(trueBOff), trueAEnd).min)
      trueCEnd must equal (List(trueAEnd, trueBEnd).min)
    }
  }
}
