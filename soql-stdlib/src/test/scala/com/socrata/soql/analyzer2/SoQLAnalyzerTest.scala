package com.socrata.soql.analyzer2

import scala.util.parsing.input.NoPosition

import java.math.{BigDecimal => JBigDecimal}

import org.scalatest.FunSuite
import org.scalatest.MustMatchers

import org.joda.time.{DateTime, LocalDate}

import com.socrata.soql.types._
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName}
import com.socrata.soql.functions.{SoQLFunctions, SoQLTypeInfo, SoQLFunctionInfo, MonomorphicFunction}

import mocktablefinder._

class SoQLAnalyzerTest extends FunSuite with MustMatchers {
  implicit val hasType = SoQLTypeInfo.hasType
  val analyzer = new SoQLAnalyzer[Int, SoQLType, SoQLValue](SoQLTypeInfo, SoQLFunctionInfo)
  def t(n: Int) = AutoTableLabel.forTest(s"t$n")
  def c(n: Int) = AutoColumnLabel.forTest(s"c$n")
  def rn(n: String) = ResourceName(n)
  def cn(n: String) = ColumnName(n)
  def dcn(n: String) = DatabaseColumnName(n)
  def dtn(n: String) = DatabaseTableName(n)

  def xtest(s: String)(f: => Any): Unit = {}

  test("simple contextless") {
    val tf = new MockTableFinder(Map.empty)

    val tf.Success(start) = tf.findTables(0, "select ((('5' + 7))), 'hello', '2001-01-01' :: date from @single_row")
    val analysis = analyzer(start, UserParameters.empty)


    analysis.statement.schema.withValuesMapped(_.name) must equal (
      OrderedMap(
        c(1) -> cn("_5_7"),
        c(2) -> cn("hello"),
        c(3) -> cn("_2001-01-01_date")
      )
    )

    val select = analysis.statement match {
      case select: Select[SoQLType, SoQLValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          FunctionCall(
            MonomorphicFunction(SoQLFunctions.BinaryPlus, Map("a" -> SoQLNumber.t)),
            Seq(
              LiteralValue(SoQLNumber(new JBigDecimal(5)))(NoPosition),
              LiteralValue(SoQLNumber(new JBigDecimal(7)))(NoPosition)
            )
          )(NoPosition, NoPosition),
          cn("_5_7")
        ),
        c(2) -> NamedExpr(
          LiteralValue(SoQLText("hello"))(NoPosition),
          cn("hello")
        ),
        c(3) -> NamedExpr(
          FunctionCall(
            SoQLFunctions.castIdentitiesByType(SoQLDate.t).monomorphic.get,
            Seq(LiteralValue(SoQLDate(LocalDate.parse("2001-01-01")))(NoPosition))
          )(NoPosition, NoPosition),
          cn("_2001-01-01_date")
        )
      )
    )

    select.from must equal (FromSingleRow(t(1), Some(rn("single_row"))))

    // and the rest is empty
    val Select(Distinctiveness.Indistinct, _, _, None, Nil, None, Nil, None, None, None, _) = select
  }

  test("simple context") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText, "num" -> SoQLNumber))
      )
    )

    val tf.Success(start) = tf.findTables(0, rn("aaaa-aaaa"), "select text as t, num * num")
    val analysis = analyzer(start, UserParameters.empty)

    analysis.statement.schema.withValuesMapped(_.name) must equal (
      OrderedMap(
        c(1) -> cn("t"),
        c(2) -> cn("num_num")
      )
    )

    val select = analysis.statement match {
      case select: Select[SoQLType, SoQLValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          Column(t(1), DatabaseColumnName("text"), SoQLText)(NoPosition),
          cn("t")
        ),
        c(2) -> NamedExpr(
          FunctionCall(
            SoQLFunctions.TimesNumNum.monomorphic.get,
            Seq(
              Column(t(1), DatabaseColumnName("num"), SoQLNumber)(NoPosition),
              Column(t(1), DatabaseColumnName("num"), SoQLNumber)(NoPosition)
            )
          )(NoPosition, NoPosition),
          cn("num_num")
        )
      )
    )

    select.from must equal (
      FromTable(
        dtn("aaaa-aaaa"),
        None,
        t(1),
        OrderedMap(
          dcn("text") -> NameEntry(cn("text"), SoQLText),
          dcn("num") -> NameEntry(cn("num"), SoQLNumber)
        )
      )
    )

    // and the rest is empty
    val Select(Distinctiveness.Indistinct, _, _, None, Nil, None, Nil, None, None, None, _) = select
  }

  test("untagged parameters in anonymous soql - impersonating a saved query") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText, "num" -> SoQLNumber))
      )
    )

    val tf.Success(start) = tf.findTables(0, rn("aaaa-aaaa"), "select param('gnu')", CanonicalName("bbbb-bbbb"))
    val analysis = analyzer(
      start,
      UserParameters(
        qualified = Map(CanonicalName("bbbb-bbbb") -> Map(HoleName("gnu") -> Right(SoQLText("Hello world"))))
      )
    )

    val select = analysis.statement match {
      case select: Select[SoQLType, SoQLValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          LiteralValue(SoQLText("Hello world"))(NoPosition),
          cn("param_gnu")
        )
      )
    )
  }

  test("untagged parameters in anonymous soql - anonymous parameters") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText, "num" -> SoQLNumber))
      )
    )

    val tf.Success(start) = tf.findTables(0, rn("aaaa-aaaa"), "select param('gnu')")
    val analysis = analyzer(
      start,
      UserParameters(
        qualified = Map.empty,
        unqualified = Right(Map(HoleName("gnu") -> Right(SoQLText("Hello world"))))
      )
    )

    val select = analysis.statement match {
      case select: Select[SoQLType, SoQLValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          LiteralValue(SoQLText("Hello world"))(NoPosition),
          cn("param_gnu")
        )
      )
    )
  }

  test("untagged parameters in anonymous soql - redirected parameters") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText, "num" -> SoQLNumber))
      )
    )

    val tf.Success(start) = tf.findTables(0, rn("aaaa-aaaa"), "select param('gnu')")
    val analysis = analyzer(
      start,
      UserParameters(
        qualified = Map(CanonicalName("bbbb-bbbb") -> Map(HoleName("gnu") -> Right(SoQLText("Hello world")))),
        unqualified = Left(CanonicalName("bbbb-bbbb"))
      )
    )

    val select = analysis.statement match {
      case select: Select[SoQLType, SoQLValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          LiteralValue(SoQLText("Hello world"))(NoPosition),
          cn("param_gnu")
        )
      )
    )
  }

  test("UDF - simple") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText, "num" -> SoQLNumber)),
        (0, "bbbb-bbbb") -> D(Map("user" -> SoQLText, "allowed" -> SoQLBoolean)),
        (0, "cccc-cccc") -> U(0, "select 1 from @bbbb-bbbb where user = ?user and allowed limit 1", OrderedMap("user" -> SoQLText))
      )
    )

    val tf.Success(start) = tf.findTables(0, rn("aaaa-aaaa"), "select * join @cccc-cccc('bob') on true")
    val analysis = analyzer(start, UserParameters.empty)

    val select = analysis.statement match {
      case select: Select[SoQLType, SoQLValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(3) -> NamedExpr(
          Column(t(1), dcn("text"), SoQLText)(NoPosition),
          cn("text")
        ),
        c(4) -> NamedExpr(
          Column(t(1), dcn("num"), SoQLNumber)(NoPosition),
          cn("num")
        )
      )
    )

    select.from must equal (
      Join(
        JoinType.Inner,
        true,
        FromTable(
          dtn("aaaa-aaaa"), None, t(1),
          OrderedMap(
            dcn("text") -> NameEntry(ColumnName("text"), SoQLText),
            dcn("num") -> NameEntry(ColumnName("num"), SoQLNumber)
          )
        ),
        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            OrderedMap(
              c(2) -> NamedExpr(
                Column(t(4),c(1),SoQLNumber)(NoPosition),
                cn("_1")
              )
            ),
            Join(
              JoinType.Inner,
              true,
              FromStatement(
                Values(Seq(LiteralValue(SoQLText("bob"))(NoPosition))),
                t(2),
                None
              ),
              FromStatement(
                Select(
                  Distinctiveness.Indistinct,
                  OrderedMap(
                    c(1) -> NamedExpr(
                      LiteralValue(SoQLNumber(new JBigDecimal(1)))(NoPosition),
                      cn("_1")
                    )
                  ),
                  FromTable(
                    DatabaseTableName("bbbb-bbbb"), Some(rn("bbbb-bbbb")), t(3),
                    OrderedMap(
                      dcn("user") -> NameEntry(cn("user"), SoQLText),
                      dcn("allowed") -> NameEntry(cn("allowed"), SoQLBoolean)
                    )
                  ),
                  Some(
                    FunctionCall(
                      SoQLFunctions.And.monomorphic.get,
                      Seq(
                        FunctionCall(
                          MonomorphicFunction(SoQLFunctions.Eq, Map("a" -> SoQLText)),
                          Seq(
                            Column(t(3),dcn("user"),SoQLText)(NoPosition),
                            Column(t(2),dcn("column1"),SoQLText)(NoPosition)
                          )
                        )(NoPosition, NoPosition),
                        Column(t(3),dcn("allowed"),SoQLBoolean)(NoPosition)
                      )
                    )(NoPosition, NoPosition)
                  ),
                  Nil, None, Nil, Some(1), None, None, Set.empty
                ),
                t(4), None
              ),
              LiteralValue(SoQLBoolean(true))(NoPosition)
            ),
            None, Nil, None, Nil, None, None, None, Set.empty
          ),
          t(5), Some(rn("cccc-cccc"))
        ),
        LiteralValue(SoQLBoolean(true))(NoPosition)
      )
    )
  }

  xtest("more complex") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText.t, "num" -> SoQLNumber.t)),
        (0, "bbbb-bbbb") -> Q(0, "aaaa-aaaa", "select text, num*2, param(@aaaa-aaaa, 'gnu')"),
        (0, "cccc-cccc") -> U(0, "select ?x from @single_row", OrderedMap("x" -> SoQLText))
      )
    )

    val tf.Success(start) = tf.findTables(0, ResourceName("bbbb-bbbb"), "select @x.*, @t.*, num_2 as bleh from @this as t join lateral @cccc-cccc(text) as x on true")

    val analysis = analyzer(start, UserParameters(qualified = Map(CanonicalName("aaaa-aaaa") -> Map(HoleName("gnu") -> Right(SoQLFixedTimestamp(DateTime.now()))))))

    //println(analysis.statement.debugStr)
    // println(analysis.statement.schema.withValuesMapped(_.name))

    // println(analysis.statement.relabel(new LabelProvider(i => s"tbl$i", c => s"col$c")).debugStr)
    // println(analysis.statement.relabel(new LabelProvider(i => s"tbl$i", c => s"col$c")).schema.withValuesMapped(_.name))
  }

  xtest("aggregates - works") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText.t, "num" -> SoQLNumber.t))
      )
    )

    val tf.Success(start) = tf.findTables(0, ResourceName("aaaa-aaaa"), "select * |> select text, sum(num) as s group by text order by s desc |> select *")

    val analysis = analyzer(start, UserParameters.empty)
    // println(analysis.statement.numericate.debugStr)
    // println(analysis.statement.schema.withValuesMapped(_.name))
  }

  xtest("udf") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText.t, "num" -> SoQLNumber.t)),
        (0, "bbbb-bbbb") -> U(0, "select num = ?x FROM @aaaa-aaaa", OrderedMap("x" -> SoQLNumber)),
        (0, "cccc-cccc") -> D(Map("name" -> SoQLText.t, "key" -> SoQLNumber.t))
      )
    )

    val tf.Success(start) = tf.findTables(0, ResourceName("cccc-cccc"), "select * join @bbbb-bbbb(key) on true")

    val analysis = analyzer(start, UserParameters.empty)

    println(analysis.statement.debugStr)
    println(analysis.statement.schema.withValuesMapped(_.name))
  }

  xtest("complicated scoping") {
    val tf = new MockTableFinder(
      Map(
        (0, "aaaa-aaaa") -> D(Map("text" -> SoQLText.t, "num" -> SoQLNumber.t)),
        (0, "bbbb-bbbb") -> U(0, "select num = ?x FROM @aaaa-aaaa", OrderedMap("x" -> SoQLNumber)),
        (0, "cccc-cccc") -> D(Map("name" -> SoQLText.t, "key" -> SoQLNumber.t))
      )
    )

    val tf.Success(start) = tf.findTables(0, ResourceName("cccc-cccc"),
       "select *, @x.* join lateral (select text from @aaaa-aaaa join @bbbb-bbbb(key) on true) as x on true")

    val analysis = analyzer(start, UserParameters.empty)
    println(analysis.statement.debugStr)
    println(analysis.statement.schema.withValuesMapped(_.name))
  }

  xtest("complicated scoping") {
    val tf = new MockTableFinder(
      Map(
        (0, "a1") -> D(Map("text" -> SoQLText.t)),
        (0, "a2") -> D(Map("text" -> SoQLText.t))
      )
    )

    val tf.Success(start) = tf.findTables(0, "select text, @w.text as t2 from @a1 join lateral (select * from @a2 as y join lateral (select text from @single_row) as z on true) as w on true")

    val analysis = analyzer(start, UserParameters.empty)
    println(analysis.statement.debugStr)
    println(analysis.statement.schema.withValuesMapped(_.name))
  }
}
