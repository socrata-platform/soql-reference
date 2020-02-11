package com.socrata.soql

import com.socrata.soql.exceptions._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks

import scala.util.parsing.input.NoPosition
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import com.socrata.soql.environment.{ColumnName, DatasetContext, ResourceName, Qualified, TableRef}
import com.socrata.soql.parsing.{Parser, StandaloneParser}
import com.socrata.soql.typechecker.Typechecker
import com.socrata.soql.types._
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.collection.{NonEmptySeq, OrderedMap}

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

  val primary = ResourceName("primary")
  val datasetCtxMap =
    Map(primary -> datasetCtx,
        ResourceName("aaaa-aaaa") -> joinCtx,
        ResourceName("aaaa-aaab") -> joinAliasCtx,
        ResourceName("aaaa-aaax") -> joinAliasWoOverlapCtx)

  def tableFinder(in: Set[ResourceName]) = datasetCtxMap.filterKeys(in)

  val analyzer = new SoQLAnalyzer(TestTypeInfo, TestFunctionInfo, tableFinder)
  type TypedExpr = typed.CoreExpr[Qualified[ColumnName], TestType]

  def expression(s: String) = new Parser().expression(s)

  sealed case class Options(primary: TableRef with TableRef.PrimaryCandidate,
                            otherCtxs: Map[ResourceName, (TableRef, Map[ColumnName, TestType])]) {
    def withPrimary(primary: TableRef with TableRef.PrimaryCandidate) =
      copy(primary = primary)

    def withSchemas(otherCtxs: (ResourceName, (TableRef, Map[ColumnName, TestType]))*) =
      copy(otherCtxs = otherCtxs.toMap)

    def withSchemas(otherCtxs: (ResourceName, (TableRef, DatasetContext[TestType]))*)(implicit erasureEvasion: Unit = ()) =
      copy(otherCtxs = otherCtxs.toMap.mapValues { case (ref, dc) => (ref, dc.schema) })
  }
  object Options extends Options(TableRef.Primary, Map.empty)

  def typedExpression(s: String, options: Options = Options): TypedExpr = {
    val tc = new Typechecker(TestTypeInfo, TestFunctionInfo)
    def convertTypes(ref: TableRef, columnName: ColumnName, columnType: TestType): TypedExpr = {
      typed.ColumnRef[Qualified[ColumnName], TestType](
        Qualified(ref, columnName),
        columnType)(NoPosition)
    }
    tc(expression(s), Typechecker.Ctx(datasetCtx.schema.transform(convertTypes(options.primary, _ ,_)),
                                      options.otherCtxs.mapValues { case (ref, schema) =>
                                        schema.transform(convertTypes(ref, _, _))
                                      }))
  }

  test("analysis succeeds in a most minimal query") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select :id")
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
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select :id as i, sum(balance) where visits > 0 group by i having sum_balance < 5 order by i desc null last, sum(balance) null first limit 5 offset 10")
    analysis.selection.toSeq must equal (Seq(ColumnName("i") -> typedExpression(":id"), ColumnName("sum_balance") -> typedExpression("sum(balance)")))
    analysis.selection(ColumnName("i")).position.column must equal (8)
    analysis.selection(ColumnName("sum_balance")).position.column must equal (18)
    analysis.where must equal (Some(typedExpression("visits > 0")))
    analysis.where.get.position.column must equal (37)
    analysis.where.get.asInstanceOf[typed.FunctionCall[_,_]].functionNamePosition.column must equal (44)
    analysis.groupBys must equal (List(typedExpression(":id")))
    analysis.groupBys.head.position.column must equal (57)
    analysis.having must equal (Some(typedExpression("sum(balance) < 5")))
    analysis.having.get.position.column must equal (66)
    analysis.having.get.asInstanceOf[typed.FunctionCall[_,_]].parameters(0).position.column must equal (66)
    analysis.having.get.asInstanceOf[typed.FunctionCall[_,_]].functionNamePosition.column must equal (78)
    analysis.orderBys must equal (List(typed.OrderBy(typedExpression(":id"), false, true), typed.OrderBy(typedExpression("sum(balance)"), true, false)))
    analysis.orderBys.map(_.expression.position.column) must equal (Seq(91, 109))
    analysis.limit must equal (Some(BigInt(5)))
    analysis.offset must equal (Some(BigInt(10)))
  }

  test("analysis succeeds in a maximal ungrouped query") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select :*, *(except name_first, name_last), nf || (' ' || nl) as name, name_first as nf, name_last as nl where nl < 'm' order by name desc, visits limit 5 offset 10")
    analysis.selection.toSeq must equal (datasetCtx.schema.toSeq.filterNot(_._1.name.startsWith("name_")).map { case (n, t) => n -> typed.ColumnRef(Qualified(TableRef.Primary, n), t)(NoPosition) } ++ Seq(ColumnName("name") -> typedExpression("name_first || (' ' || name_last)"), ColumnName("nf") -> typedExpression("name_first"), ColumnName("nl") -> typedExpression("name_last")))
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
    analysis.where.get.asInstanceOf[typed.FunctionCall[_,_]].parameters(0).position.column must equal (112)
    analysis.groupBys must equal (Nil)
    analysis.having must equal (None)
    analysis.orderBys must equal (List(typed.OrderBy(typedExpression("name_first || (' ' || name_last)"), false, false), typed.OrderBy(typedExpression("visits"), true, true)))
    analysis.orderBys.map(_.expression.position.column) must equal (Seq(130, 141))
    analysis.limit must equal (Some(BigInt(5)))
    analysis.offset must equal (Some(BigInt(10)))
  }

  test("analysis succeeds in cast") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select name_last::number as c1, '123'::number as c2, 456::text as c3")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("c1") -> typedExpression("name_last::number"),
      ColumnName("c2") -> typedExpression("'123'::number"),
      ColumnName("c3") -> typedExpression("456::text")
    ))
  }

  test("json property and index") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select object.xxxx as c1, array[123] as c2, object.yyyy::text as c3, array[123]::number as c4")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("c1") -> typedExpression("object.xxxx"),
      ColumnName("c2") -> typedExpression("array[123]"),
      ColumnName("c3") -> typedExpression("object.yyyy::text"),
      ColumnName("c4") -> typedExpression("array[123]::number")
    ))
  }

  test("subcolumn subscript converted") {
    val soql = "select address.human_address where address.latitude > 1.1 order by address.longitude"
    val analysis = analyzer.analyzeUnchainedQuery(primary, soql)
    val typedCol = typed.ColumnRef(Qualified(TableRef.Primary,
                                             ColumnName("address")),
                                   TestLocation.t)(NoPosition)

    analysis.selection.toSeq must equal (Seq(
      ColumnName("address_human_address") -> typed.FunctionCall(TestFunctions.LocationToAddress.monomorphic.get, Seq(typedCol))(NoPosition, NoPosition)
    ))
    analysis.where must equal (Some(typed.FunctionCall(MonomorphicFunction(TestFunctions.Gt, Map("a" -> TestNumber)), Seq(typed.FunctionCall(TestFunctions.LocationToLatitude.monomorphic.get, Seq(typedCol))(NoPosition, NoPosition), typed.NumberLiteral(new java.math.BigDecimal("1.1"), TestNumber.t)(NoPosition)))(NoPosition, NoPosition)))
    analysis.where.get.position.column must equal (36)
    analysis.orderBys must equal (List(typed.OrderBy(typed.FunctionCall(TestFunctions.LocationToLongitude.monomorphic.get, Seq(typedCol))(NoPosition, NoPosition), true, true)))
  }

  test("null :: number succeeds") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select null :: number as x")
    analysis.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestNumber)).get.monomorphic.get,
      Seq(typed.NullLiteral(TestNumber.t)(NoPosition)))(NoPosition, NoPosition))
  }

  test("5 :: number succeeds") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select 5 :: number as x")
    analysis.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestNumber)).get.monomorphic.get,
      Seq(typed.NumberLiteral(java.math.BigDecimal.valueOf(5), TestNumber.t)(NoPosition)))(NoPosition, NoPosition))
  }

  test("5 :: money succeeds") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select 5 :: money as x")
    analysis.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestMoney)).get.monomorphic.get,
      Seq(typed.FunctionCall(TestFunctions.NumberToMoney.monomorphic.get, Seq(typed.NumberLiteral(java.math.BigDecimal.valueOf(5), TestNumber.t)(NoPosition)))(NoPosition, NoPosition)))(NoPosition, NoPosition))
  }

  test("a subselect makes the output of the inner select available to the outer") {
    val NonEmptySeq(inner, Seq(outer)) = analyzer.analyzeFullQuery(primary, "select 5 :: money as x |> select max(x)")
    outer.selection(ColumnName("max_x")) must equal (typed.FunctionCall(MonomorphicFunction(TestFunctions.Max, Map("a" -> TestMoney)), Seq(typed.ColumnRef(Qualified(TableRef.PreviousChainStep(TableRef.Primary, 1), ColumnName("x")), TestMoney.t)(NoPosition)))(NoPosition, NoPosition))
    inner.selection(ColumnName("x")) must equal (typed.FunctionCall(TestFunctions.castIdentities.find(_.result == functions.FixedType(TestMoney)).get.monomorphic.get,
      Seq(typed.FunctionCall(TestFunctions.NumberToMoney.monomorphic.get, Seq(typed.NumberLiteral(java.math.BigDecimal.valueOf(5), TestNumber.t)(NoPosition)))(NoPosition, NoPosition)))(NoPosition, NoPosition))
  }

  test("cannot ORDER BY an unorderable type") {
    an [UnorderableOrderBy] must be thrownBy analyzer.analyzeFullQuery(primary, "select * order by array")
  }

  test("cannot filter by a non-boolean type") {
    a [NonBooleanWhere] must be thrownBy analyzer.analyzeFullQuery(primary, "select * where array")
    a [NonBooleanHaving] must be thrownBy analyzer.analyzeFullQuery(primary, "select count(*) having count(*)")
  }

  test("cannot group by a non-groupable type") {
    a [NonGroupableGroupBy] must be thrownBy analyzer.analyzeFullQuery(primary, "select array group by array")
  }

  test("Merging two simple filter queries is the same as querying one") {
    val analysis1 = analyzer.analyzeFullQuery(primary, "select 2*visits as twice_visits where twice_visits > 10 |> select * where twice_visits > 20")
    val analysis2 = analyzer.analyzeFullQuery(primary, "select 2*visits as twice_visits where twice_visits > 10 and twice_visits > 20")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging a filter-on-a-group-on-a-filter is the same as a where-group-having one") {
    val analysis1 = analyzer.analyzeFullQuery(primary, "select visits where visits > 10 |> select visits, count(*) as c group by visits |> select * where c > 5")
    val analysis2 = analyzer.analyzeFullQuery(primary, "select visits, count(*) as c where visits > 10 group by visits having c > 5")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging limits truncates the second limit to the window defined by the first") {
    val analysis1 = analyzer.analyzeFullQuery(primary, "select visits offset 10 limit 5 |> select * offset 3 limit 10")
    val analysis2 = analyzer.analyzeFullQuery(primary, "select visits offset 13 limit 2")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging an offset to the end of a limit reduces the limit to 0") {
    val analysis1 = analyzer.analyzeFullQuery(primary, "select visits offset 10 limit 5 |> select * offset 5")
    val analysis2 = analyzer.analyzeFullQuery(primary, "select visits offset 15 limit 0")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging an offset past the end of a limit reduces the limit to 0 and does not move the offset past the end of the first query") {
    val analysis1 = analyzer.analyzeFullQuery(primary, "select visits offset 10 limit 5 |> select * offset 50")
    val analysis2 = analyzer.analyzeFullQuery(primary, "select visits offset 15 limit 0")
    SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1) must equal (analysis2)
  }

  test("Merging join") {
    val analysis1 = analyzer.analyzeFullQuery(primary, "select name_last, visits where visits > 1 |> select visits, @aaaa-aaaa.name_last join @aaaa-aaaa on @aaaa-aaaa.name_last = name_last where @aaaa-aaaa.name_last='Almond'")
    val analysis2 = analyzer.analyzeFullQuery(primary, "select visits, @aaaa-aaaa.name_last join @aaaa-aaaa on @aaaa-aaaa.name_last = name_last where visits > 1 and @aaaa-aaaa.name_last='Almond'")
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1)
    merged must equal (analysis2)
  }

  test("Merging join with table alias") {
    val analysis1 = analyzer.analyzeFullQuery(primary, "select name_last, name_first, visits where visits > 1 |> select visits as vis, @a1.name_first join @aaaa-aaab as a1 on @a1.name_first = name_first where @a1.name_first='John'")
    val analysis2 = analyzer.analyzeFullQuery(primary, "select visits as vis, @a1.name_first join @aaaa-aaab as a1 on @a1.name_first = name_first where visits > 1 and @a1.name_first='John'")
    val merged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis1)
    merged must equal (analysis2)
  }

  test("Back to back joins do not merge") {
    val analysis = analyzer.analyzeFullQuery(primary, "select name_first, @aaaa-aaaa.name_last as name_last join @aaaa-aaaa on @aaaa-aaaa.name_last = name_last |> select name_last, @a1.name_first join @aaaa-aaab as a1 on @a1.name_first = name_first")
    val notMerged = SoQLAnalysis.merge(TestFunctions.And.monomorphic.get, analysis)
    notMerged must equal (analysis)
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
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select distinct name_last, visits")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("name_last") -> typedExpression("name_last"),
      ColumnName("visits") -> typedExpression("visits")
    ))
    analysis.distinct must be (true)
  }

  test("alias reuse") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select name_last as last_name, last_name as ln")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("last_name") -> typedExpression("name_last"),
      ColumnName("ln") -> typedExpression("name_last")
    ))
  }

  test("qualified column name") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select object.a, object.b as ob, visits, @aaaa-aaaa.name_last, @a1.name_last join @aaaa-aaaa on true join @aaaa-aaaa as a1 on true")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("object_a") -> typedExpression("object.a"),
      ColumnName("ob") -> typedExpression("object.b"),
      ColumnName("visits") -> typedExpression("visits"),
      ColumnName("aaaa_aaaa_name_last") -> typedExpression("@aaaa-aaaa.name_last", Options.withSchemas(ResourceName("aaaa-aaaa") -> ((TableRef.Join(0), datasetCtxMap(ResourceName("aaaa-aaaa")))))),
      ColumnName("a1_name_last") -> typedExpression("@a1.name_last", Options.withSchemas(ResourceName("a1") -> ((TableRef.Join(1), datasetCtxMap(ResourceName("aaaa-aaaa"))))))
    ))
  }

  test("join") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select visits, @aaaa-aaaa.name_last join @aaaa-aaaa on name_last = @aaaa-aaaa.name_last")
    val visit = typedExpression("visits").asInstanceOf[ColumnRef[Qualified[ColumnName], _]]
    visit.column.table must equal(TableRef.Primary)
    val joinTable = ResourceName("aaaa-aaaa")
    val lastName= typedExpression("@aaaa-aaaa.name_last", Options.withSchemas(joinTable -> ((TableRef.Join(0), datasetCtxMap(joinTable))))).asInstanceOf[ColumnRef[Qualified[ColumnName], _]]
    lastName.column.table must equal(TableRef.Join(0))
    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> visit,
      ColumnName("aaaa_aaaa_name_last") -> lastName
    ))
    analysis.joins must equal (List(typed.InnerJoin(JoinAnalysis(joinTable, 0, Nil),
                                                    typedExpression("name_last = @aaaa-aaaa.name_last",
                                                                    Options.withSchemas(joinTable -> ((TableRef.Join(0), datasetCtxMap(joinTable))))))))
  }

  test("join with table alias") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select visits, @a1.name_last join @aaaa-aaaa as a1 on visits > 10")
    val joinTable = ResourceName("aaaa-aaaa")
    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> typedExpression("visits"),
      ColumnName("a1_name_last") -> typedExpression("@aaaa-aaaa.name_last", Options.withSchemas(joinTable -> ((TableRef.Join(0), datasetCtxMap(joinTable)))))
    ))
    analysis.joins must equal (List(typed.InnerJoin(JoinAnalysis(joinTable, 0, Nil),
                                                    typedExpression("visits > 10",
                                                                    Options.withSchemas(joinTable -> ((TableRef.Join(0), datasetCtxMap(joinTable))))))))
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
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select :*, *, @x1.* join @aaaa-aaax as x1 on visits > 10")
    val joinTable = ResourceName("aaaa-aaax")
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
      ColumnName("x1_x") -> typedExpression("@aaaa-aaax.x", Options.withSchemas(joinTable -> ((TableRef.Join(0), datasetCtxMap(joinTable))))),
      ColumnName("x1_y") -> typedExpression("@aaaa-aaax.y", Options.withSchemas(joinTable -> ((TableRef.Join(0), datasetCtxMap(joinTable))))),
      ColumnName("x1_z") -> typedExpression("@aaaa-aaax.z", Options.withSchemas(joinTable -> ((TableRef.Join(0), datasetCtxMap(joinTable)))))
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

  def parseSubselectJoin(joinSoql: String): NonEmptySeq[SoQLAnalysis[Qualified[ColumnName], TestType]] = {
    val parsed = new StandaloneParser().parseSubselectJoinSource(joinSoql)
    analyzer.analyze(parsed.fromTable.resourceName, parsed.subSelect, TableRef.JoinPrimary(parsed.fromTable.resourceName, 0))
  }

  test("join with sub-query") {
    val joinSubSoqlInner = "select * from @aaaa-aaab where name_first = 'xxx'"
    val joinSubSoql = s"($joinSubSoqlInner) as a1"
    val subAnalyses = parseSubselectJoin(joinSubSoql)
    val analysis = analyzer.analyzeUnchainedQuery(primary, s"select visits, @a1.name_first join $joinSubSoql on name_first = @a1.name_first")
    val ctx = Options.withSchemas(ResourceName("a1") -> ((TableRef.Join(0), datasetCtxMap(ResourceName("aaaa-aaab")))))
    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> typedExpression("visits", ctx),
      ColumnName("a1_name_first") -> typedExpression("@a1.name_first", ctx)
    ))

    val expected = List(typed.InnerJoin(JoinAnalysis(ResourceName("aaaa-aaab"), 0, subAnalyses.seq),
                                        typedExpression("name_first = @a1.name_first", ctx)))

    analysis.joins must equal (expected)
  }

  test("join with sub-chained-query") {
    val joinSubSoqlInner = "select * from @aaaa-aaab where name_first = 'aaa' |> select * where name_first = 'bbb'"
    val joinSubSoql = s"($joinSubSoqlInner) as a1"
    val subAnalyses = parseSubselectJoin(joinSubSoql)
    val analysis = analyzer.analyzeUnchainedQuery(primary, s"select visits, @a1.name_first join $joinSubSoql on name_first = @a1.name_first")
    val ctx = Options.withSchemas(ResourceName("a1") -> ((TableRef.Join(0), datasetCtxMap(ResourceName("aaaa-aaab")))))
    analysis.selection.toSeq must equal (Seq(
      ColumnName("visits") -> typedExpression("visits", ctx),
      ColumnName("a1_name_first") -> typedExpression("@a1.name_first", ctx)
    ))
    val expected = List(typed.InnerJoin(JoinAnalysis(ResourceName("aaaa-aaab"), 0, subAnalyses.seq),
                                        typedExpression("name_first = @a1.name_first", ctx)))

    analysis.joins must equal (expected)
  }

  test("nested join") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, s"""
SELECT visits, @x3.x
  JOIN (SELECT @x2.x AS x, @a1.name_first FROM @aaaa-aaab as a1
          JOIN (SELECT @x1.x AS x FROM @aaaa-aaax as x1) as x2 on @x2.x = @a1.name_first
       ) as x3 on @x3.x = name_first
      """)

    val expected =
      SoQLAnalysis(false,
                   false,
                   OrderedMap(
                     ColumnName("visits") -> typed.ColumnRef(Qualified(
                                                               TableRef.Primary,
                                                               ColumnName("visits")),
                                                             TestNumber.t)(NoPosition),
                     ColumnName("x3_x") -> typed.ColumnRef(Qualified(
                                                             TableRef.Join(1),
                                                             ColumnName("x")),
                                                           TestText.t)(NoPosition)),
                   Seq(typed.InnerJoin(
                         JoinAnalysis(ResourceName("aaaa-aaab"),
                                      1,
                                      Seq(SoQLAnalysis(
                                            false,
                                            false,
                                            OrderedMap(
                                              ColumnName("x") -> typed.ColumnRef(Qualified(
                                                                                   TableRef.Join(0),
                                                                                   ColumnName("x")),
                                                                                 TestText.t)(NoPosition),
                                              ColumnName("a1_name_first") -> typed.ColumnRef(Qualified(
                                                                                               TableRef.JoinPrimary(ResourceName("aaaa-aaab"), 1),
                                                                                               ColumnName("name_first")),
                                                                                             TestText.t)(NoPosition)),
                                            Seq(typed.InnerJoin(
                                                  JoinAnalysis(
                                                    ResourceName("aaaa-aaax"),
                                                    0,
                                                    Seq(SoQLAnalysis(
                                                          false,
                                                          false,
                                                          OrderedMap(
                                                            ColumnName("x") -> typed.ColumnRef(Qualified(
                                                                                                 TableRef.JoinPrimary(ResourceName("aaaa-aaax"), 0),
                                                                                                 ColumnName("x")),
                                                                                               TestText.t)(NoPosition)),
                                                          Nil,
                                                          None,
                                                          Nil,
                                                          None,
                                                          Nil,
                                                          None,
                                                          None,
                                                          None))),
                                                  typed.FunctionCall(MonomorphicFunction(
                                                                       TestFunctions.Eq,
                                                                       Map("a" -> TestText)),
                                                                     Seq(
                                                                       typed.ColumnRef(Qualified(
                                                                                         TableRef.Join(0),
                                                                                         ColumnName("x")),
                                                                                       TestText.t)(NoPosition),
                                                                       typed.ColumnRef(Qualified(
                                                                                         TableRef.JoinPrimary(ResourceName("aaaa-aaab"), 1),
                                                                                         ColumnName("name_first")),
                                                                                       TestText.t)(NoPosition)))(NoPosition, NoPosition))),
                                            None,
                                            Nil,
                                            None,
                                            Nil,
                                            None,
                                            None,
                                            None))),
                         typed.FunctionCall(MonomorphicFunction(
                                              TestFunctions.Eq,
                                              Map("a" -> TestText)),
                                            Seq(
                                              typed.ColumnRef(Qualified(
                                                                TableRef.Join(1),
                                                                ColumnName("x")),
                                                              TestText.t)(NoPosition),
                                              typed.ColumnRef(Qualified(
                                                                TableRef.Primary,
                                                                ColumnName("name_first")),
                                                              TestText.t)(NoPosition)))(NoPosition, NoPosition))),
                   None,
                   Nil,
                   None,
                   Nil,
                   None,
                   None,
                   None)

    analysis must equal (expected)
  }

  test("nested join re-using table alias - x2") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, s"""
SELECT visits, @x2.zx
 RIGHT OUTER JOIN (SELECT @x2.x as zx FROM @aaaa-aaab as a1
          LEFT OUTER JOIN (SELECT @x1.x as x FROM @aaaa-aaax as x1) as x2 on @x2.x = @a1.name_first
       ) as x2 on @x2.zx = name_first
      """)

    val expected =
      SoQLAnalysis(false,
                   false,
                   OrderedMap(
                     ColumnName("visits") -> typed.ColumnRef(Qualified(
                                                               TableRef.Primary,
                                                               ColumnName("visits")),
                                                             TestNumber.t)(NoPosition),
                     ColumnName("x3_x") -> typed.ColumnRef(Qualified(
                                                             TableRef.Join(1),
                                                             ColumnName("x")),
                                                           TestText.t)(NoPosition)),
                   Seq(typed.RightOuterJoin(
                         JoinAnalysis(ResourceName("aaaa-aaab"),
                                      1,
                                      Seq(SoQLAnalysis(
                                            false,
                                            false,
                                            OrderedMap(
                                              ColumnName("x") -> typed.ColumnRef(Qualified(
                                                                                   TableRef.Join(0),
                                                                                   ColumnName("x")),
                                                                                 TestText.t)(NoPosition),
                                              ColumnName("a1_name_first") -> typed.ColumnRef(Qualified(
                                                                                               TableRef.JoinPrimary(ResourceName("aaaa-aaab"), 1),
                                                                                               ColumnName("name_first")),
                                                                                             TestText.t)(NoPosition)),
                                            Seq(typed.LeftOuterJoin(
                                                  JoinAnalysis(
                                                    ResourceName("aaaa-aaax"),
                                                    0,
                                                    Seq(SoQLAnalysis(
                                                          false,
                                                          false,
                                                          OrderedMap(
                                                            ColumnName("x") -> typed.ColumnRef(Qualified(
                                                                                                 TableRef.JoinPrimary(ResourceName("aaaa-aaax"), 0),
                                                                                                 ColumnName("x")),
                                                                                               TestText.t)(NoPosition)),
                                                          Nil,
                                                          None,
                                                          Nil,
                                                          None,
                                                          Nil,
                                                          None,
                                                          None,
                                                          None))),
                                                  typed.FunctionCall(MonomorphicFunction(
                                                                       TestFunctions.Eq,
                                                                       Map("a" -> TestText)),
                                                                     Seq(
                                                                       typed.ColumnRef(Qualified(
                                                                                         TableRef.Join(0),
                                                                                         ColumnName("x")),
                                                                                       TestText.t)(NoPosition),
                                                                       typed.ColumnRef(Qualified(
                                                                                         TableRef.JoinPrimary(ResourceName("aaaa-aaab"), 1),
                                                                                         ColumnName("name_first")),
                                                                                       TestText.t)(NoPosition)))(NoPosition, NoPosition))),
                                            None,
                                            Nil,
                                            None,
                                            Nil,
                                            None,
                                            None,
                                            None))),
                         typed.FunctionCall(MonomorphicFunction(
                                              TestFunctions.Eq,
                                              Map("a" -> TestText)),
                                            Seq(
                                              typed.ColumnRef(Qualified(
                                                                TableRef.Join(1),
                                                                ColumnName("x")),
                                                              TestText.t)(NoPosition),
                                              typed.ColumnRef(Qualified(
                                                                TableRef.Primary,
                                                                ColumnName("name_first")),
                                                              TestText.t)(NoPosition)))(NoPosition, NoPosition))),
                   None,
                   Nil,
                   None,
                   Nil,
                   None,
                   None,
                   None)
  }

  test("window function") {
    val analysisWordStyle = analyzer.analyzeUnchainedQuery(primary, "SELECT name_last, avg(visits) OVER (PARTITION BY name_last, 2, 3)")
    analysisWordStyle.isGrouped must equal (false)
    val select = analysisWordStyle.selection.toSeq
    select must equal (Seq(
      ColumnName("name_last") -> typedExpression("name_last"),
      ColumnName("avg_visits_over_partition_by_name_last_2_3") -> typedExpression("avg(visits) OVER (PARTITION BY name_last, 2, 3)")
    ))

    val analysisWordStyleOverEmpty = analyzer.analyzeUnchainedQuery(primary, "SELECT avg(visits) OVER ()")
    val selectOverEmpty = analysisWordStyleOverEmpty.selection.toSeq
    selectOverEmpty must equal (Seq(
      ColumnName("avg_visits_over") -> typedExpression("avg(visits) OVER ()")
    ))

    val analysis = analyzer.analyzeUnchainedQuery(primary, "SELECT avg(visits)")
    analysis.isGrouped must equal (true)
    analysis.selection.toSeq must equal (Seq(
      ColumnName("avg_visits") -> typedExpression("avg(visits)")
    ))

    intercept[TypeMismatch] {
      analyzer.analyzeUnchainedQuery(primary, "SELECT avg(name_last)")
    }

    val expressionExpected = intercept[BadParse] {
      analyzer.analyzeUnchainedQuery(primary, "SELECT name_last, avg(visits) OVER (PARTITION BY)")
    }
    expressionExpected.message must startWith("Expression expected")


    val partitionExpected = intercept[BadParse] {
      analyzer.analyzeUnchainedQuery(primary, "SELECT name_last, avg(visits) OVER (name_last, 2, 3)")
    }
    partitionExpected.message must startWith("`)' expected")
  }

  test("aliases work") {
    val analysisWordStyle = analyzer.analyzeUnchainedQuery(
      primary,
      "SELECT datez_trunc_ymd(:created_at) as dt, date_trunc_ymd(:created_at, 'PDT') as dt_pdt")
    analysisWordStyle.isGrouped must equal(false)
    val select = analysisWordStyle.selection.toSeq
    select must equal(Seq(
      ColumnName("dt") -> typed.FunctionCall(TestFunctions.FixedTimeStampZTruncYmd.monomorphic.get,
        Seq(typed.ColumnRef(Qualified(TableRef.Primary,
                                      ColumnName(":created_at")),
                            TestFixedTimestamp.t)(NoPosition)))(NoPosition, NoPosition),
      ColumnName("dt_pdt") -> typed.FunctionCall(TestFunctions.FixedTimeStampTruncYmdAtTimeZone.monomorphic.get,
        Seq(typed.ColumnRef(Qualified(TableRef.Primary,
                                      ColumnName(":created_at")),
                            TestFixedTimestamp.t)(NoPosition),
            typed.StringLiteral("PDT", TestText.t)(NoPosition)
           )
        )
        (NoPosition, NoPosition)
    ))
  }

  test("joined table name alias is part of default function-column name") {
    val analysis = analyzer.analyzeUnchainedQuery(primary, "select sum(@a1.:id) join @aaaa-aaab as a1 on :id = @a1.:id")
    analysis.selection.head._1.name must equal("sum_a1_id")
  }

}
