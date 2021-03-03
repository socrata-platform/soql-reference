package com.socrata.soql

import com.socrata.NonEmptySeq
import com.socrata.soql.exceptions._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks

import scala.util.parsing.input.NoPosition
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.parsing.{Parser, StandaloneParser}
import com.socrata.soql.typechecker.Typechecker
import com.socrata.soql.types._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typed.ColumnRef

class SoQLAnalyzerTest extends FunSuite with MustMatchers with PropertyChecks {
  val datasetCtx = new DatasetContext[TestType] {
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

  val joinCtx = new DatasetContext[TestType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> TestNumber,
      ColumnName(":updated_at") -> TestFixedTimestamp,
      ColumnName(":created_at") -> TestFixedTimestamp,
      ColumnName("name_last") -> TestText
    )
  }

  val joinAliasCtx = new DatasetContext[TestType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> TestNumber,
      ColumnName(":updated_at") -> TestFixedTimestamp,
      ColumnName(":created_at") -> TestFixedTimestamp,
      ColumnName("name_first") -> TestText
    )
  }

  val joinAliasWoOverlapCtx = new DatasetContext[TestType] {
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> TestNumber,
      ColumnName(":updated_at") -> TestFixedTimestamp,
      ColumnName(":created_at") -> TestFixedTimestamp,
      ColumnName("x") -> TestText,
      ColumnName("y") -> TestText,
      ColumnName("z") -> TestText
    )
  }

  implicit val datasetCtxMap =
    Map(TableName.PrimaryTable.qualifier -> datasetCtx,
        TableName("_aaaa-aaaa", None).qualifier -> joinCtx,
        TableName("_aaaa-aaab", Some("_a1")).qualifier -> joinAliasCtx,
        TableName("_aaaa-aaax", Some("_x1")).qualifier -> joinAliasWoOverlapCtx,
        TableName("_aaaa-aaab", None).qualifier -> joinAliasCtx,
        TableName("_aaaa-aaax", None).qualifier -> joinAliasWoOverlapCtx)

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
    analysis.groupBys must be (Nil)
    analysis.having must be (None)
    analysis.orderBys must be (Nil)
    analysis.limit must be (None)
    analysis.offset must be (None)
    analysis.isGrouped must be (false)
    analysis.distinct must be (false)
  }

  test("analysis succeeds in a maximal group-by query") {
    val analysis = analyzer.analyzeUnchainedQuery("select :id as i, sum(balance) where visits > 0 group by i having sum_balance < 5 order by i desc null last, sum(balance) null first limit 5 offset 10")
    analysis.selection.toSeq must equal (Seq(ColumnName("i") -> typedExpression(":id"), ColumnName("sum_balance") -> typedExpression("sum(balance)")))
    analysis.selection(ColumnName("i")).position.column must equal (8)
    analysis.selection(ColumnName("sum_balance")).position.column must equal (18)
    analysis.where must equal (Some(typedExpression("visits > 0")))
    analysis.where.get.position.column must equal (37)
    analysis.where.get.asInstanceOf[typed.FunctionCall[_,_]].functionNamePosition.column must equal (44)
    analysis.groupBys must equal (List(typedExpression(":id")))
    analysis.groupBys.head.position.column must equal (8)
    analysis.having must equal (Some(typedExpression("sum(balance) < 5")))
    analysis.having.get.position.column must equal (66)
    analysis.having.get.asInstanceOf[typed.FunctionCall[_,_]].parameters(0).position.column must equal (18)
    analysis.having.get.asInstanceOf[typed.FunctionCall[_,_]].functionNamePosition.column must equal (78)
    analysis.orderBys must equal (List(typed.OrderBy(typedExpression(":id"), false, true), typed.OrderBy(typedExpression("sum(balance)"), true, false)))
    analysis.orderBys.map(_.expression.position.column) must equal (Seq(8, 109))
    analysis.limit must equal (Some(BigInt(5)))
    analysis.offset must equal (Some(BigInt(10)))
  }

  test("analysis succeeds in a maximal ungrouped query") {
    val analysis = analyzer.analyzeUnchainedQuery("select :*, *(except name_first, name_last), nf || (' ' || nl) as name, name_first as nf, name_last as nl where nl < 'm' order by name desc, visits limit 5 offset 10")
    analysis.selection.toSeq must equal (datasetCtx.schema.toSeq.filterNot(_._1.name.startsWith("name_")).map { case (n, t) => n -> typed.ColumnRef(None, n, t)(NoPosition) } ++ Seq(ColumnName("name") -> typedExpression("name_first || (' ' || name_last)"), ColumnName("nf") -> typedExpression("name_first"), ColumnName("nl") -> typedExpression("name_last")))
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
    analysis.groupBys must equal (Nil)
    analysis.having must equal (None)
    analysis.orderBys must equal (List(typed.OrderBy(typedExpression("name_first || (' ' || name_last)"), false, false), typed.OrderBy(typedExpression("visits"), true, true)))
    analysis.orderBys.map(_.expression.position.column) must equal (Seq(45, 141))
    analysis.limit must equal (Some(BigInt(5)))
    analysis.offset must equal (Some(BigInt(10)))
  }

  test("Giving no values to the split-query analyzer returns the equivalent of `SELECT *'") {
    val analysis = analyzer.analyzeSplitQuery(false, None, None, None, None, None, None, None, None, None)(Map(TableName.PrimaryTable.qualifier -> datasetCtx))
    analysis must equal (analyzer.analyzeUnchainedQuery("SELECT *"))
  }

  test("Putting an aggregate in the order-by slot causes aggregation to occur") {
    val analysis = analyzer.analyzeSplitQuery(false, None, None, None, None, None, Some("max(visits)"), None, None, None)
    analysis must equal (analyzer.analyzeUnchainedQuery("SELECT count(*) ORDER BY max(visits)"))
  }

  test("Having a group by clause puts them in the selection list") {
    val computed = "name_first || ' ' || name_last"
    val sep = ", "
    val uncomputed = "visits"
    val analysis = analyzer.analyzeSplitQuery(false, None, None, None, Some(computed + sep + uncomputed), None, None, None, None, None)
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

  test("subcolumn subscript converted") {
    val soql = "select address.human_address where address.latitude > 1.1 order by address.longitude"
    val analysis = analyzer.analyzeUnchainedQuery(soql)
    val typedCol = typed.ColumnRef(None, ColumnName("address"), TestLocation.t)(NoPosition)

    analysis.selection.toSeq must equal (Seq(
      ColumnName("address_human_address") -> typed.FunctionCall(TestFunctions.LocationToAddress.monomorphic.get, Seq(typedCol), None)(NoPosition, NoPosition)
    ))
    analysis.where must equal (Some(typed.FunctionCall(MonomorphicFunction(TestFunctions.Gt, Map("a" -> TestNumber)), Seq(typed.FunctionCall(TestFunctions.LocationToLatitude.monomorphic.get, Seq(typedCol), None)(NoPosition, NoPosition), typed.NumberLiteral(new java.math.BigDecimal("1.1"), TestNumber.t)(NoPosition)), None)(NoPosition, NoPosition)))
    analysis.where.get.position.column must equal (36)
    analysis.orderBys must equal (List(typed.OrderBy(typed.FunctionCall(TestFunctions.LocationToLongitude.monomorphic.get, Seq(typedCol), None)(NoPosition, NoPosition), true, true)))
  }

  test("null :: number succeeds") {
    val analysis = analyzer.analyzeUnchainedQuery("select null :: number as x")
    analysis.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestNumber)).get.monomorphic.get,
      Seq(typed.NullLiteral(TestNumber.t)(NoPosition)), None)(NoPosition, NoPosition))
  }

  test("5 :: number succeeds") {
    val analysis = analyzer.analyzeUnchainedQuery("select 5 :: number as x")
    analysis.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestNumber)).get.monomorphic.get,
      Seq(typed.NumberLiteral(java.math.BigDecimal.valueOf(5), TestNumber.t)(NoPosition)), None)(NoPosition, NoPosition))
  }

  test("5 :: money succeeds") {
    val analysis = analyzer.analyzeUnchainedQuery("select 5 :: money as x")
    analysis.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestMoney)).get.monomorphic.get,
      Seq(typed.FunctionCall(TestFunctions.NumberToMoney.monomorphic.get, Seq(typed.NumberLiteral(java.math.BigDecimal.valueOf(5), TestNumber.t)(NoPosition)), None)(NoPosition, NoPosition)), None)(NoPosition, NoPosition))
  }

  test("a subselect makes the output of the inner select available to the outer") {
    val PipeQuery(inner, outer) = analyzer.analyzeFullQueryBinary("select 5 :: money as x |> select max(x)")
    outer.asLeaf.get.selection(ColumnName("max_x")) must equal (typed.FunctionCall(MonomorphicFunction(TestFunctions.Max, Map("a" -> TestMoney)), Seq(typed.ColumnRef(None, ColumnName("x"), TestMoney : TestType)(NoPosition)), None)(NoPosition, NoPosition))
    inner.asLeaf.get.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestMoney)).get.monomorphic.get,
      Seq(typed.FunctionCall(TestFunctions.NumberToMoney.monomorphic.get, Seq(typed.NumberLiteral(java.math.BigDecimal.valueOf(5), TestNumber.t)(NoPosition)), None)(NoPosition, NoPosition)), None)(NoPosition, NoPosition))
  }

  test("cannot ORDER BY an unorderable type") {
    an [UnorderableOrderBy] must be thrownBy analyzer.analyzeFullQueryBinary("select * order by array")
  }

  test("cannot filter by a non-boolean type") {
    a [NonBooleanWhere] must be thrownBy analyzer.analyzeFullQueryBinary("select * where array")
    a [NonBooleanHaving] must be thrownBy analyzer.analyzeFullQueryBinary("select count(*) having count(*)")
  }

  test("cannot group by a non-groupable type") {
    a [NonGroupableGroupBy] must be thrownBy analyzer.analyzeFullQueryBinary("select array group by array")
  }

  test("Merging two simple filter queries is the same as querying one") {
    val analysis1 = analyzer.analyzeFullQueryBinary("select 2*visits as twice_visits where twice_visits > 10 |> select * where twice_visits > 20")
    val analysis2 = analyzer.analyzeFullQueryBinary("select 2*visits as twice_visits where twice_visits > 10 and twice_visits > 20")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging a filter-on-a-group-on-a-filter is the same as a where-group-having one") {
    val analysis1 = analyzer.analyzeFullQueryBinary("select visits where visits > 10 |> select visits, count(*) as c group by visits |> select * where c > 5")
    val analysis2 = analyzer.analyzeFullQueryBinary("select visits, count(*) as c where visits > 10 group by visits having c > 5")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging limits truncates the second limit to the window defined by the first") {
    val analysis1 = analyzer.analyzeFullQueryBinary("select visits offset 10 limit 5 |> select * offset 3 limit 10")
    val analysis2 = analyzer.analyzeFullQueryBinary("select visits offset 13 limit 2")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }


  test("Merging an offset to the end of a limit reduces the limit to 0") {
    val analysis1 = analyzer.analyzeFullQueryBinary("select visits offset 10 limit 5 |> select * offset 5")
    val analysis2 = analyzer.analyzeFullQueryBinary("select visits offset 15 limit 0")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging an offset past the end of a limit reduces the limit to 0 and does not move the offset past the end of the first query") {
    val analysis1 = analyzer.analyzeFullQueryBinary("select visits offset 10 limit 5 |> select * offset 50")
    val analysis2 = analyzer.analyzeFullQueryBinary("select visits offset 15 limit 0")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging join") {
    val analysis1 = analyzer.analyzeFullQueryBinary("select name_last, visits where visits > 1 |> select visits, @aaaa-aaaa.name_last join @aaaa-aaaa on @aaaa-aaaa.name_last = name_last where @aaaa-aaaa.name_last='Almond'")
    val analysis2 = analyzer.analyzeFullQueryBinary("select visits, @aaaa-aaaa.name_last join @aaaa-aaaa on @aaaa-aaaa.name_last = name_last where visits > 1 and @aaaa-aaaa.name_last='Almond'")
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1)
    merged must equal (analysis2)
  }

  test("Merging join with column alias of the main line") {
    val analysis1 = analyzer.analyzeFullQueryBinary("select coalesce(name_last, name_first) as nl, visits where visits > 1 |> select visits, @aaaa-aaaa.name_last join @aaaa-aaaa on @aaaa-aaaa.name_last = nl where @aaaa-aaaa.name_last='Almond'")
    val analysis2 = analyzer.analyzeFullQueryBinary("select visits, @aaaa-aaaa.name_last join @aaaa-aaaa on @aaaa-aaaa.name_last = coalesce(name_last, name_first) where visits > 1 and @aaaa-aaaa.name_last='Almond'")
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1)
    merged must equal (analysis2)
  }

  test("Merging join with table alias") {
    val analysis1 = analyzer.analyzeFullQueryBinary("select name_last, name_first, visits where visits > 1 |> select visits as vis, @a1.name_first join @aaaa-aaab as a1 on @a1.name_first = name_first where @a1.name_first='John'")
    val analysis2 = analyzer.analyzeFullQueryBinary("select visits as vis, @a1.name_first join @aaaa-aaab as a1 on @a1.name_first = name_first where visits > 1 and @a1.name_first='John'")
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1)
    merged must equal (analysis2)
  }

  test("Back to back joins do not merge") {
    val analysis = analyzer.analyzeFullQueryBinary("select name_first, @aaaa-aaaa.name_last join @aaaa-aaaa on @aaaa-aaaa.name_last = name_last |> select name_last, @a1.name_first join @aaaa-aaab as a1 on @a1.name_first = name_first")
    val notMerged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    notMerged must equal (analysis)
  }

  test("Upper non-mergeable does not throw away lower merge") {
    val soqlMerged = "SELECT name_first, name_last WHERE name_first=name_last |> SELECT name_first SEARCH 'first'"
    val soql = "SELECT name_first, name_last |> " + soqlMerged
    val analysis = analyzer.analyzeFullQueryBinary(soql)
    val expected = analyzer.analyzeFullQueryBinary(soqlMerged)
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    merged must equal (expected)
  }

  test("Union part merges") {
    val soqlCommon = "(SELECT name_first, name_last |> SELECT name_first, name_last WHERE name_first=name_last)"
    val soql = soqlCommon + " UNION " + soqlCommon
    val analysis = analyzer.analyzeFullQueryBinary(soql)
    val soqlMerged = "SELECT name_first, name_last WHERE name_first=name_last UNION SELECT name_first, name_last WHERE name_first=name_last"
    val expected = analyzer.analyzeFullQueryBinary(soqlMerged)
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    merged must equal (expected)
  }

  test("Union parts merge") {
    val soql = "SELECT name_first |> SELECT name_first UNION (SELECT name_first FROM @aaaa-aaab |> SELECT name_first)"
    val soqlMerged = "SELECT name_first UNION SELECT name_first FROM @aaaa-aaab"
    val analysis = analyzer.analyzeFullQueryBinary(soql)
    val expected = analyzer.analyzeFullQueryBinary(soqlMerged)
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    merged must equal (expected)
  }

  test("Non consecutives merge") {
    val soql = "SELECT name_first, :id |> SELECT name_first, :id JOIN @aaaa-aaab as a1 on true |> SELECT name_first, 'one', 'two', :id |> SELECT name_first, `one`, :id"
    val soqlMerged = "SELECT name_first, :id JOIN @aaaa-aaab as a1 on true |> SELECT name_first, 'one', :id"
    val analysis = analyzer.analyzeFullQueryBinary(soql)
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    val expected = analyzer.analyzeFullQueryBinary(soqlMerged)
    merged must equal (expected)
  }

  test("Union does not merge with chain") {
    val soql = "SELECT name_first, name_last UNION SELECT 'one', 'two' FROM @aaaa-aaab |> SELECT name_first"
    val analysis = analyzer.analyzeFullQueryBinary(soql)
    val notMerged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    notMerged must equal (analysis)
  }

  test("All merge to a leaf with window function") {
    val soql = "SELECT name_first, visits, 1 |> SELECT name_first, visits, 2 |> SELECT name_first, visits, 3 |> SELECT name_first, avg(visits) over()"
    val soqlMerged = "SELECT name_first, avg(visits) over()"
    val analysis = analyzer.analyzeFullQueryBinary(soql)
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    val expected = analyzer.analyzeFullQueryBinary(soqlMerged)
    merged must equal (expected)
  }

  test("All merge to a leaf with search") {
    val soql = "SELECT name_first SEARCH 'tom' |> SELECT name_first |> SELECT name_first |> SELECT name_first"
    val soqlMerged = "SELECT name_first SEARCH 'tom'"
    val analysis = analyzer.analyzeFullQueryBinary(soql)
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    val expected = analyzer.analyzeFullQueryBinary(soqlMerged)
    merged must equal (expected)
  }

  test("Consecutives merge except the last") {
    val soql = "SELECT name_first |> SELECT name_first |> SELECT name_first |> SELECT name_first SEARCH 'tom'"
    val soqlMerged = "SELECT name_first |> SELECT name_first SEARCH 'tom'"
    val analysis = analyzer.analyzeFullQueryBinary(soql)
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    val expected = analyzer.analyzeFullQueryBinary(soqlMerged)
    merged must equal (expected)
  }

  test("Consecutives merge except the first") {
    val soql = "SELECT name_first JOIN @aaaa-aaab as a1 on true SEARCH 'tom' |> SELECT name_first |> SELECT name_first |> SELECT name_first"
    val soqlMerged = "SELECT name_first JOIN @aaaa-aaab as a1 on true SEARCH 'tom' |> SELECT name_first"
    val analysis = analyzer.analyzeFullQueryBinary(soql)
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    val expected = analyzer.analyzeFullQueryBinary(soqlMerged)
    merged must equal (expected)
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

  test("distinct") {
    val analysis = analyzer.analyzeUnchainedQuery("select distinct name_last, visits")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("name_last") -> typedExpression("name_last"),
      ColumnName("visits") -> typedExpression("visits")
    ))
    analysis.distinct must be (true)
  }

  test("alias reuse") {
    val analysis = analyzer.analyzeUnchainedQuery("select name_last as last_name, last_name as ln")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("last_name") -> typedExpression("name_last"),
      ColumnName("ln") -> typedExpression("name_last")
    ))
  }

  test("qualified column name") {
    val analysis = analyzer.analyzeUnchainedQuery("select object.a, object.b as ob, visits, @aaaa-aaaa.name_last, @a1.name_first")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("object_a") -> typedExpression("object.a"),
      ColumnName("ob") -> typedExpression("object.b"),
      ColumnName("visits") -> typedExpression("visits"),
      ColumnName("name_last") -> typedExpression("@aaaa-aaaa.name_last"),
      ColumnName("name_first") -> typedExpression("@a1.name_first")
    ))
  }

  test("join") {
    val analysis = analyzer.analyzeUnchainedQuery("select visits, @aaaa-aaaa.name_last join @aaaa-aaaa on name_last = @aaaa-aaaa.name_last")
    val visit: ColumnRef[_, _] = typedExpression("visits").asInstanceOf[ColumnRef[_, _]]
    visit.qualifier must equal(None)
    val lastName: ColumnRef[_, _] = typedExpression("@aaaa-aaaa.name_last").asInstanceOf[ColumnRef[_, _]]
    lastName.qualifier must equal(Some("_aaaa-aaaa"))
    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> visit,
      ColumnName("name_last") -> lastName
    ))
    analysis.joins must equal (List(typed.InnerJoin(JoinAnalysis(Left(TableName("_aaaa-aaaa"))), typedExpression("name_last = @aaaa-aaaa.name_last"))))
  }

  test("join with table alias") {
    val analysis = analyzer.analyzeUnchainedQuery("select visits, @a1.name_first join @aaaa-aaab as a1 on visits > 10")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> typedExpression("visits"),
      ColumnName("name_first") -> typedExpression("@a1.name_first")
    ))
    analysis.joins must equal (List(typed.InnerJoin(JoinAnalysis(Left(TableName("_aaaa-aaab", Some("_a1")))), typedExpression("visits > 10"))))
  }

  test("join toString") {
    val soql = "select visits, @a1.name_first join @aaaa-aaaa as a1 on name_last = @a1.name_last"
    val parsed = new Parser().unchainedSelectStatement(soql)

    val expected = "SELECT `visits`, @a1.`name_first` JOIN @aaaa-aaaa AS a1 ON `name_last` = @a1.`name_last`"
    parsed.toString must equal(expected)

    val parsedAgain = new Parser().unchainedSelectStatement(expected)
    parsedAgain.toString must equal(expected)
  }

  test("left outer join toString") {
    val soql = "select visits, @a1.name_first left outer join @aaaa-aaaa as a1 on name_last = @a1.name_last"
    val parsed = new Parser().unchainedSelectStatement(soql)

    val expected = "SELECT `visits`, @a1.`name_first` LEFT OUTER JOIN @aaaa-aaaa AS a1 ON `name_last` = @a1.`name_last`"
    parsed.toString must equal(expected)

    val parsedAgain = new Parser().unchainedSelectStatement(expected)
    parsedAgain.toString must equal(expected)
  }

  test("right outer join toString") {
    val soql = "select visits, @a1.name_first right outer join @aaaa-aaaa as a1 on name_last = @a1.name_last"
    val parsed = new Parser().unchainedSelectStatement(soql)

    val expected = "SELECT `visits`, @a1.`name_first` RIGHT OUTER JOIN @aaaa-aaaa AS a1 ON `name_last` = @a1.`name_last`"
    parsed.toString must equal(expected)

    val parsedAgain = new Parser().unchainedSelectStatement(expected)
    parsedAgain.toString must equal(expected)
  }

  test("multiple `SELECT *`'s") {
    val analysis = analyzer.analyzeUnchainedQuery("select :*, *, @x1.* join @aaaa-aaax as x1 on visits > 10")
    analysis.selection.toSeq must equal (Seq(
      ColumnName(":id") -> typedExpression(":id"),
      ColumnName(":updated_at") -> typedExpression(":updated_at"),
      ColumnName(":created_at") -> typedExpression(":created_at"),
      ColumnName("name_last") -> typedExpression("name_last"),
      ColumnName("name_first") -> typedExpression("name_first"),
      ColumnName("visits") -> typedExpression("visits"),
      ColumnName("last_visit") -> typedExpression("last_visit"),
      ColumnName("address") -> typedExpression("address"),
      ColumnName("balance") -> typedExpression("balance"),
      ColumnName("object") -> typedExpression("object"),
      ColumnName("array") -> typedExpression("array"),
      ColumnName("x") -> typedExpression("@x1.x"),
      ColumnName("y") -> typedExpression("@x1.y"),
      ColumnName("z") -> typedExpression("@x1.z")
    ))
  }

  test("`SELECT *` toString") {
    val soql = "SELECT @a1.:*, @a1.*, * JOIN @aaaa-aaax AS x1 ON visits > 10"
    val parsed = new Parser().unchainedSelectStatement(soql)

    val expected = "SELECT @a1.:*, @a1.*, * JOIN @aaaa-aaax AS x1 ON `visits` > 10"
    parsed.toString must equal(expected)

    val parsedAgain = new Parser().unchainedSelectStatement(expected)
    parsedAgain.toString must equal(expected)
  }

  def parseJoin(joinSoql: String)(implicit ctx: analyzer.AnalysisContext): BinaryTree[SoQLAnalysis[ColumnName, TestType]] = {
    val parsed = new StandaloneParser().parseJoinSelect(joinSoql)
    analyzer.analyzeBinary(parsed.selects.get)(ctx)
  }

  test("join with sub-query") {
    val joinSubSoqlInner = "select * from @aaaa-aaab where name_first = 'xxx'"
    val joinSubSoql = s"($joinSubSoqlInner) as a1"
    val subAnalyses = parseJoin(joinSubSoql)(datasetCtxMap + (TableName.PrimaryTable.qualifier -> joinAliasCtx))
    val analysis = analyzer.analyzeUnchainedQuery(s"select visits, @a1.name_first join $joinSubSoql on name_first = @a1.name_first")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> typedExpression("visits"),
      ColumnName("name_first") -> typedExpression("@a1.name_first")
    ))
    val expected = List(typed.InnerJoin(JoinAnalysis(Right(SubAnalysis(subAnalyses, "_a1"))), typedExpression("name_first = @a1.name_first")))
    analysis.joins must equal (expected)
  }

  test("join with sub-chained-query") {
    val joinSubSoqlInner = "select * from @aaaa-aaab where name_first = 'aaa' |> select * where name_first = 'bbb'"
    val joinSubSoql = s"($joinSubSoqlInner) as a1"
    val subAnalyses = parseJoin(joinSubSoql)(datasetCtxMap + (TableName.PrimaryTable.qualifier -> joinAliasCtx))
    val analysis = analyzer.analyzeUnchainedQuery(s"select visits, @a1.name_first join $joinSubSoql on name_first = @a1.name_first")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> typedExpression("visits"),
      ColumnName("name_first") -> typedExpression("@a1.name_first")
    ))
    val expected = List(typed.InnerJoin(JoinAnalysis(Right(SubAnalysis(subAnalyses, "_a1"))), typedExpression("name_first = @a1.name_first")))
    analysis.joins must equal (expected)
  }

  test("nested join") {
    val analysis = analyzer.analyzeUnchainedQuery(s"""
SELECT visits, @x3.x
  JOIN (SELECT @x2.x, @a1.name_first FROM @aaaa-aaab as a1
          JOIN (SELECT @x1.x FROM @aaaa-aaax as x1) as x2 on @x2.x = @a1.name_first
       ) as x3 on @x3.x = name_first
      """)

    val innermostJoins = analysis.joins.head.from.subAnalysis.right.get.analyses.asLeaf.get.joins
    val innermostAnalysis = innermostJoins.head.from.subAnalysis.right.get.analyses.asLeaf.get
    innermostAnalysis.selection.toSeq must equal (Seq(
      ColumnName("x") -> ColumnRef(Some("_x1"), ColumnName("x"), TestText)(NoPosition)
    ))

    val joins = analysis.joins
    val joinAnalysis = joins.head.from.subAnalysis.right.get.analyses.asLeaf.get
    joinAnalysis.selection.toSeq must equal (Seq(
      ColumnName("x") -> ColumnRef(Some("_x2"), ColumnName("x"), TestText)(NoPosition),
      ColumnName("name_first") -> ColumnRef(Some("_a1"), ColumnName("name_first"), TestText)(NoPosition)
    ))

    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> ColumnRef(None, ColumnName("visits"), TestNumber)(NoPosition),
      ColumnName("x") -> ColumnRef(Some("_x3"), ColumnName("x"), TestText)(NoPosition)
    ))
  }

  test("nested join re-using table alias - x2") {
    val analysis = analyzer.analyzeUnchainedQuery(s"""
SELECT visits, @x2.zx
 RIGHT OUTER JOIN (SELECT @x2.x as zx FROM @aaaa-aaab as a1
          LEFT OUTER JOIN (SELECT @x1.x FROM @aaaa-aaax as x1) as x2 on @x2.x = @a1.name_first
       ) as x2 on @x2.zx = name_first
      """)

    val innermostLeftOuterJoin = analysis.joins.head.from.analyses.get.asLeaf.get.joins
    val innermostAnalysis = innermostLeftOuterJoin.head.from.analyses.get.asLeaf.get
    innermostAnalysis.selection.toSeq must equal (Seq(
      ColumnName("x") -> ColumnRef(Some("_x1"), ColumnName("x"), TestText)(NoPosition)
    ))

    val rightOuterJoinAnalysis = analysis.joins.head.from.analyses.get.asLeaf.get
    rightOuterJoinAnalysis.selection.toSeq must equal (Seq(
      ColumnName("zx") -> ColumnRef(Some("_x2"), ColumnName("x"), TestText)(NoPosition)
    ))

    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> ColumnRef(None, ColumnName("visits"), TestNumber)(NoPosition),
      ColumnName("zx") -> ColumnRef(Some("_x2"), ColumnName("zx"), TestText)(NoPosition)
    ))
  }

  test("window function") {
    val analysisWordStyle = analyzer.analyzeUnchainedQuery("SELECT name_last, avg(visits) OVER (PARTITION BY name_last, 2, 3)")
    analysisWordStyle.isGrouped must equal (false)
    val select = analysisWordStyle.selection.toSeq
    select must equal (Seq(
      ColumnName("name_last") -> typedExpression("name_last"),
      ColumnName("avg_visits_over_partition_by_name_last_2_3") -> typedExpression("avg(visits) OVER (PARTITION BY name_last, 2, 3)")
    ))

    val analysisWordStyleOverEmpty = analyzer.analyzeUnchainedQuery("SELECT avg(visits) OVER ()")
    val selectOverEmpty = analysisWordStyleOverEmpty.selection.toSeq
    selectOverEmpty must equal (Seq(
      ColumnName("avg_visits_over") -> typedExpression("avg(visits) OVER ()")
    ))

    val analysis = analyzer.analyzeUnchainedQuery("SELECT avg(visits)")
    analysis.isGrouped must equal (true)
    analysis.selection.toSeq must equal (Seq(
      ColumnName("avg_visits") -> typedExpression("avg(visits)")
    ))

    intercept[TypeMismatch] {
      analyzer.analyzeUnchainedQuery("SELECT avg(name_last)")
    }

    val expressionExpected = intercept[BadParse] {
      analyzer.analyzeUnchainedQuery("SELECT name_last, avg(visits) OVER (PARTITION BY)")
    }
    expressionExpected.message must startWith("Expression expected")


    val partitionExpected = intercept[BadParse] {
      analyzer.analyzeUnchainedQuery("SELECT name_last, avg(visits) OVER (name_last, 2, 3)")
    }
    partitionExpected.message must startWith("`)' expected")
  }

  test("aliases work") {
    val analysisWordStyle = analyzer.analyzeUnchainedQuery(
      "SELECT datez_trunc_ymd(:created_at) as dt, date_trunc_ymd(:created_at, 'PDT') as dt_pdt")
    analysisWordStyle.isGrouped must equal(false)
    val select = analysisWordStyle.selection.toSeq
    select must equal(Seq(
      ColumnName("dt") -> typed.FunctionCall(TestFunctions.FixedTimeStampZTruncYmd.monomorphic.get,
        Seq(typed.ColumnRef(None, ColumnName(":created_at"), TestFixedTimestamp.t)(NoPosition)), None)(NoPosition, NoPosition),
      ColumnName("dt_pdt") -> typed.FunctionCall(TestFunctions.FixedTimeStampTruncYmdAtTimeZone.monomorphic.get,
        Seq(typed.ColumnRef(None, ColumnName(":created_at"), TestFixedTimestamp.t)(NoPosition),
            typed.StringLiteral("PDT", TestText.t)(NoPosition)
           ),
        None
        )
        (NoPosition, NoPosition)
    ))
  }

  test("joined table name alias is part of default function-column name") {
    val analysis = analyzer.analyzeUnchainedQuery("select sum(@a1.:id) join @aaaa-aaab as a1 on :id = @a1.:id")
    analysis.selection.head._1.name must equal("sum_a1_id")
  }

  test("Merge window function with group by aggregate") {
    val analysis1 = analyzer.analyzeFullQueryBinary("SELECT name_first, name_last, sum(visits) as n2 GROUP BY name_first, name_last |> SELECT name_first, row_number() over(order by n2) as rn")
    val analysis2 = analyzer.analyzeFullQueryBinary("SELECT name_first, row_number() over(order by sum(visits)) as rn GROUP BY name_first, name_last")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }
}
