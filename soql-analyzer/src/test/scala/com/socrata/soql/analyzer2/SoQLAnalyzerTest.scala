package com.socrata.soql.analyzer2

import scala.util.parsing.input.NoPosition

import java.math.{BigDecimal => JBigDecimal}

import org.scalatest.{FunSuite, MustMatchers}

import org.joda.time.{DateTime, LocalDate}

import com.socrata.soql.collection._
import com.socrata.soql.functions.MonomorphicFunction

import mocktablefinder._

class SoQLAnalyzerTest extends FunSuite with MustMatchers with TestHelper {
  test("alias analysis qualifiers are correct") {
    val tf = MockTableFinder.empty[Int, TestType]
    val Right(start) = tf.findTables(0, "select @bleh.whatever from @single_row", Map.empty)
    analyzer(start, UserParameters.empty) match {
      case Right(_) => fail("Expected an error")
      case Left(SoQLAnalyzerError.TextualError(_, _, _, nsc: SoQLAnalyzerError.AnalysisError.TypecheckError.NoSuchColumn)) =>
        nsc.qualifier must be (Some(rn("bleh")))
        nsc.name must be (cn("whatever"))
      case Left(err) =>
        fail(s"Wrong error: ${err}")
    }
  }

  test("simple contextless") {
    val tf = MockTableFinder.empty[Int, TestType]

    // Getting rid of parents, literal coersions, function overloading, disambiguation....
    val analysis = analyze(tf, "select ((('5' + 7))), 'hello' + 'world', '1' + '2' from @single_row")

    analysis.statement.schema.withValuesMapped(_.name) must equal (
      OrderedMap(
        c(1) -> cn("_5_7"),
        c(2) -> cn("hello_world"),
        c(3) -> cn("_1_2")
      )
    )

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          FunctionCall(
            MonomorphicFunction(TestFunctions.BinaryPlus, Map("a" -> TestNumber)),
            Seq(
              LiteralValue(TestNumber(5))(AtomicPositionInfo.None),
              LiteralValue(TestNumber(7))(AtomicPositionInfo.None)
            )
          )(FuncallPositionInfo.None),
          cn("_5_7")
        ),
        c(2) -> NamedExpr(
          FunctionCall(
            MonomorphicFunction(TestFunctions.BinaryPlus, Map("a" -> TestText)),
            Seq(
              LiteralValue(TestText("hello"))(AtomicPositionInfo.None),
              LiteralValue(TestText("world"))(AtomicPositionInfo.None)
            )
          )(FuncallPositionInfo.None),
          cn("hello_world")
        ),
        c(3) -> NamedExpr(
          FunctionCall(
            MonomorphicFunction(TestFunctions.BinaryPlus, Map("a" -> TestText)),
            Seq(
              LiteralValue(TestText("1"))(AtomicPositionInfo.None),
              LiteralValue(TestText("2"))(AtomicPositionInfo.None)
            )
          )(FuncallPositionInfo.None),
          cn("_1_2")
        )
      )
    )

    select.from must equal (FromSingleRow(t(1), Some(rn("single_row"))))

    // and the rest is empty
    val Select(Distinctiveness.Indistinct, _, _, None, Nil, None, Nil, None, None, None, _) = select
  }

  test("simple context") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select text as t, num * num")

    analysis.statement.schema.withValuesMapped(_.name) must equal (
      OrderedMap(
        c(1) -> cn("t"),
        c(2) -> cn("num_num")
      )
    )

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          Column(t(1), dcn("text"), TestText)(AtomicPositionInfo.None),
          cn("t")
        ),
        c(2) -> NamedExpr(
          FunctionCall(
            TestFunctions.Times.monomorphic.get,
            Seq(
              Column(t(1), dcn("num"), TestNumber)(AtomicPositionInfo.None),
              Column(t(1), dcn("num"), TestNumber)(AtomicPositionInfo.None)
            )
          )(FuncallPositionInfo.None),
          cn("num_num")
        )
      )
    )

    select.from must equal (
      FromTable(
        dtn("aaaa-aaaa"),
        ScopedResourceName(0, rn("aaaa-aaaa")),
        None,
        t(1),
        OrderedMap(
          dcn("text") -> NameEntry(cn("text"), TestText),
          dcn("num") -> NameEntry(cn("num"), TestNumber)
        ),
        Nil
      )
    )

    // and the rest is empty
    val Select(Distinctiveness.Indistinct, _, _, None, Nil, None, Nil, None, None, None, _) = select
  }

  test("untagged parameters in anonymous soql - impersonating a saved query") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(
      tf,
      "aaaa-aaaa",
      "select param('gnu')",
      CanonicalName("bbbb-bbbb"),
      UserParameters(
        qualified = Map(CanonicalName("bbbb-bbbb") -> Map(hn("gnu") -> UserParameters.Value(TestText("Hello world"))))
      )
    )

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          LiteralValue(TestText("Hello world"))(AtomicPositionInfo.None),
          cn("param_gnu")
        )
      )
    )
  }

  test("untagged parameters in anonymous soql - anonymous parameters") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select param('gnu')",
      UserParameters(
        qualified = Map.empty,
        unqualified = Map(hn("gnu") -> UserParameters.Value(TestText("Hello world")))
      )
    )

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          LiteralValue(TestText("Hello world"))(AtomicPositionInfo.None),
          cn("param_gnu")
        )
      )
    )
  }

  test("untagged parameters in anonymous soql - redirected parameters") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select param('gnu')", CanonicalName("bbbb-bbbb"),
      UserParameters(
        qualified = Map(CanonicalName("bbbb-bbbb") -> Map(hn("gnu") -> UserParameters.Value(TestText("Hello world"))))
      )
    )

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          LiteralValue(TestText("Hello world"))(AtomicPositionInfo.None),
          cn("param_gnu")
        )
      )
    )
  }

  test("dataset ordering - direct select") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber).withOrdering("text")
    )

    val analysis = analyzeSaved(tf, "aaaa-aaaa")

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select must equal (
      Select(
        Distinctiveness.Indistinct,
        OrderedMap(
          c(1) -> NamedExpr(Column(t(1), dcn("text"), TestText)(AtomicPositionInfo.None), cn("text")),
          c(2) -> NamedExpr(Column(t(1), dcn("num"), TestNumber)(AtomicPositionInfo.None), cn("num"))
        ),
        FromTable(
          dtn("aaaa-aaaa"),
          ScopedResourceName(0, rn("aaaa-aaaa")),
          None,
          t(1),
          OrderedMap(
            dcn("text") -> NameEntry(cn("text"), TestText),
            dcn("num") -> NameEntry(cn("num"), TestNumber)
          ),
          Nil
        ),
        None, Nil, None,
        List(
          OrderBy(Column(t(1), dcn("text"), TestText)(AtomicPositionInfo.None),true,true)
        ),
        None,None,None,Set.empty
      )
    )
  }


  test("dataset ordering - nested select") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber).withOrdering("text")
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select *")

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select must equal (
      Select(
        Distinctiveness.Indistinct,
        OrderedMap(
          c(3) -> NamedExpr(Column(t(2),c(1),TestText)(AtomicPositionInfo.None),cn("text")),
          c(4) -> NamedExpr(Column(t(2),c(2),TestNumber)(AtomicPositionInfo.None),cn("num"))
        ),
        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            OrderedMap(
              c(1) -> NamedExpr(Column(t(1), dcn("text"), TestText)(AtomicPositionInfo.None), cn("text")),
              c(2) -> NamedExpr(Column(t(1), dcn("num"), TestNumber)(AtomicPositionInfo.None), cn("num"))
            ),
            FromTable(
              dtn("aaaa-aaaa"),
              ScopedResourceName(0, rn("aaaa-aaaa")),
              None,
              t(1),
              OrderedMap(
                dcn("text") -> NameEntry(cn("text"), TestText),
                dcn("num") -> NameEntry(cn("num"), TestNumber)
              ),
              Nil
            ),
            None, Nil, None,
            List(
              OrderBy(Column(t(1), dcn("text"), TestText)(AtomicPositionInfo.None),true,true)
            ),
            None,None,None,Set.empty
          ),
          t(2),
          Some(ScopedResourceName(0, rn("aaaa-aaaa"))),
          None
        ),
        None,Nil,None,Nil,None,None,None,Set.empty
      )
    )
  }

  test("distinct on - ignores permutations") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber)
    )
    analyze(tf, "aaaa-aaaa", "select distinct on (text, num) 5 order by num, text")
    // didn't throw an exception, good
  }

  test("distinct on - requires prefix match") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber)
    )

    expectFailure(0) {
      analyze(tf, "aaaa-aaaa", "select distinct on (text, num) 5 order by num, num*2, text")(new OnFail {
        override def onAnalyzerError(e: AnalysisError[Int]): Nothing = {
          e match {
            case SoQLAnalyzerError.TextualError(_, _, _, SoQLAnalyzerError.AnalysisError.DistinctNotPrefixOfOrderBy) =>
              expected(0)
            case _ =>
              super.onAnalyzerError(e)
          }
        }
      })
    }
  }

  test("distinct on - allows extra order bys") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber)
    )

    analyze(tf, "aaaa-aaaa", "select distinct on (text, num) 5 order by num, text, num*2")
  }

  test("UDF - simple") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "bbbb-bbbb") -> D("user" -> TestText, "allowed" -> TestBoolean),
      (0, "cccc-cccc") -> U(0, "select 1 from @bbbb-bbbb where user = ?user and allowed limit 1", "user" -> TestText)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select * join @cccc-cccc('bob') on true")

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(3) -> NamedExpr(
          Column(t(1), dcn("text"), TestText)(AtomicPositionInfo.None),
          cn("text")
        ),
        c(4) -> NamedExpr(
          Column(t(1), dcn("num"), TestNumber)(AtomicPositionInfo.None),
          cn("num")
        )
      )
    )

    select.from must equal (
      Join(
        JoinType.Inner,
        true,
        FromTable(
          dtn("aaaa-aaaa"), ScopedResourceName(0, rn("aaaa-aaaa")), None, t(1),
          OrderedMap(
            dcn("text") -> NameEntry(cn("text"), TestText),
            dcn("num") -> NameEntry(cn("num"), TestNumber)
          ),
          Nil
        ),
        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            OrderedMap(
              c(2) -> NamedExpr(Column(t(4),c(1),TestNumber)(AtomicPositionInfo.None), cn("_1"))
            ),
            Join(
              JoinType.Inner,
              true,
              FromStatement(
                Values(NonEmptySeq(NonEmptySeq(LiteralValue(TestText("bob"))(AtomicPositionInfo.None),Nil),Nil)),
                t(2), None, None
              ),
              FromStatement(
                Select(
                  Distinctiveness.Indistinct,
                  OrderedMap(
                    c(1) -> NamedExpr(LiteralValue(TestNumber(1))(AtomicPositionInfo.None),cn("_1"))
                  ),
                  FromTable(
                    dtn("bbbb-bbbb"), ScopedResourceName(0, rn("bbbb-bbbb")),Some(rn("bbbb-bbbb")),
                    t(3),
                    OrderedMap(
                      dcn("user") -> NameEntry(cn("user"),TestText),
                      dcn("allowed") -> NameEntry(cn("allowed"),TestBoolean)
                    ),
                    Nil
                  ),
                  Some(
                    FunctionCall(
                      TestFunctions.And.monomorphic.get,
                      Seq(
                        FunctionCall(
                          MonomorphicFunction(TestFunctions.Eq, Map("a" -> TestText)),
                          Seq(
                            Column(t(3),dcn("user"),TestText)(AtomicPositionInfo.None),
                            Column(t(2),dcn("column1"),TestText)(AtomicPositionInfo.None)
                          )
                        )(FuncallPositionInfo.None),
                        Column(t(3),dcn("allowed"),TestBoolean)(AtomicPositionInfo.None)
                      )
                    )(FuncallPositionInfo.None)
                  ),
                  Nil,None,Nil,Some(1),None,None,Set.empty
                ),
                t(4), None, None
              ),
              LiteralValue(TestBoolean(true))(AtomicPositionInfo.None)
            ),
            None,Nil,None,Nil,None,None,None,Set.empty
          ),
          t(5),
          Some(ScopedResourceName(0,rn("cccc-cccc"))),Some(rn("cccc-cccc"))
        ),
        LiteralValue(TestBoolean(true))(AtomicPositionInfo.None)
      )
    )
  }

  test("UDF - referencing outer column") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "bbbb-bbbb") -> D("user" -> TestText, "allowed" -> TestBoolean),
      (0, "cccc-cccc") -> U(0, "select 1 from @bbbb-bbbb where user = ?user and allowed limit 1", "user" -> TestText)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select * join @cccc-cccc(text) on true")

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(3) -> NamedExpr(
          Column(t(1), dcn("text"), TestText)(AtomicPositionInfo.None),
          cn("text")
        ),
        c(4) -> NamedExpr(
          Column(t(1), dcn("num"), TestNumber)(AtomicPositionInfo.None),
          cn("num")
        )
      )
    )

    select.from must equal (
      Join(
        JoinType.Inner,
        true,
        FromTable(
          dtn("aaaa-aaaa"), ScopedResourceName(0, rn("aaaa-aaaa")), None, t(1),
          OrderedMap(
            dcn("text") -> NameEntry(cn("text"), TestText),
            dcn("num") -> NameEntry(cn("num"), TestNumber)
          ),
          Nil
        ),
        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            OrderedMap(
              c(2) -> NamedExpr(Column(t(4),c(1),TestNumber)(AtomicPositionInfo.None), cn("_1"))
            ),
            Join(
              JoinType.Inner,
              true,
              FromStatement(
                Values(NonEmptySeq(NonEmptySeq(Column(t(1),dcn("text"),TestText)(AtomicPositionInfo.None),Nil),Nil)),
                t(2), None, None
              ),
              FromStatement(
                Select(
                  Distinctiveness.Indistinct,
                  OrderedMap(
                    c(1) -> NamedExpr(LiteralValue(TestNumber(1))(AtomicPositionInfo.None),cn("_1"))
                  ),
                  FromTable(
                    dtn("bbbb-bbbb"), ScopedResourceName(0, rn("bbbb-bbbb")),Some(rn("bbbb-bbbb")),
                    t(3),
                    OrderedMap(
                      dcn("user") -> NameEntry(cn("user"),TestText),
                      dcn("allowed") -> NameEntry(cn("allowed"),TestBoolean)
                    ),
                    Nil
                  ),
                  Some(
                    FunctionCall(
                      TestFunctions.And.monomorphic.get,
                      Seq(
                        FunctionCall(
                          MonomorphicFunction(TestFunctions.Eq, Map("a" -> TestText)),
                          Seq(
                            Column(t(3),dcn("user"),TestText)(AtomicPositionInfo.None),
                            Column(t(2),dcn("column1"),TestText)(AtomicPositionInfo.None)
                          )
                        )(FuncallPositionInfo.None),
                        Column(t(3),dcn("allowed"),TestBoolean)(AtomicPositionInfo.None)
                      )
                    )(FuncallPositionInfo.None)
                  ),
                  Nil,None,Nil,Some(1),None,None,Set.empty
                ),
                t(4), None, None
              ),
              LiteralValue(TestBoolean(true))(AtomicPositionInfo.None)
            ),
            None,Nil,None,Nil,None,None,None,Set.empty
          ),
          t(5),
          Some(ScopedResourceName(0,rn("cccc-cccc"))),Some(rn("cccc-cccc"))
        ),
        LiteralValue(TestBoolean(true))(AtomicPositionInfo.None)
      )
    )
  }

  test("UDF - some parameters are function calls") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "bbbb-bbbb") -> U(0, "select ?a, ?b, ?c, ?d from @single_row", "d" -> TestText, "c" -> TestNumber, "b" -> TestBoolean, "a" -> TestText)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select * join @bbbb-bbbb('hello' :: text, 5, true :: boolean, text) on true")

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(9) -> NamedExpr(
          Column(t(1), dcn("text"), TestText)(AtomicPositionInfo.None),
          cn("text")
        ),
        c(10) -> NamedExpr(
          Column(t(1), dcn("num"), TestNumber)(AtomicPositionInfo.None),
          cn("num")
        )
      )
    )

    select.from must equal (
      Join(
        JoinType.Inner,
        true,
        FromTable(
          dtn("aaaa-aaaa"), ScopedResourceName(0, rn("aaaa-aaaa")), None, t(1),
          OrderedMap(
            dcn("text") -> NameEntry(cn("text"), TestText),
            dcn("num") -> NameEntry(cn("num"), TestNumber)
          ),
          Nil
        ),
        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            OrderedMap(
              c(5) -> NamedExpr(Column(t(4), c(1), TestText)(AtomicPositionInfo.None), cn("a")),
              c(6) -> NamedExpr(Column(t(4), c(2), TestBoolean)(AtomicPositionInfo.None), cn("b")),
              c(7) -> NamedExpr(Column(t(4), c(3), TestNumber)(AtomicPositionInfo.None), cn("c")),
              c(8) -> NamedExpr(Column(t(4), c(4), TestText)(AtomicPositionInfo.None), cn("d"))
            ),
            Join(
              JoinType.Inner,
              true,
              FromStatement(
                Values(
                  NonEmptySeq(
                    NonEmptySeq(
                      FunctionCall(
                        TestFunctions.castIdentitiesByType(TestText).monomorphic.get,
                        Seq(LiteralValue(TestText("hello"))(AtomicPositionInfo.None))
                      )(FuncallPositionInfo.None),
                      Seq(
                        LiteralValue(TestNumber(5))(AtomicPositionInfo.None),
                        FunctionCall(
                          TestFunctions.castIdentitiesByType(TestBoolean).monomorphic.get,
                          Seq(LiteralValue(TestBoolean(true))(AtomicPositionInfo.None))
                        )(FuncallPositionInfo.None),
                        Column(t(1), dcn("text"), TestText)(AtomicPositionInfo.None)
                      )
                    )
                  )
                ),
                t(2), None, None
              ),
              FromStatement(
                Select(
                  Distinctiveness.Indistinct,
                  OrderedMap(
                    c(1) -> NamedExpr(Column(t(2),dcn("column4"),TestText)(AtomicPositionInfo.None), cn("a")),
                    c(2) -> NamedExpr(Column(t(2),dcn("column3"),TestBoolean)(AtomicPositionInfo.None), cn("b")),
                    c(3) -> NamedExpr(Column(t(2),dcn("column2"),TestNumber)(AtomicPositionInfo.None), cn("c")),
                    c(4) -> NamedExpr(Column(t(2),dcn("column1"),TestText)(AtomicPositionInfo.None), cn("d"))
                  ),
                  FromSingleRow(t(3), Some(rn("single_row"))),
                  None,Nil,None,Nil,None,None,None,Set()
                ),
                t(4), None, None
              ),
              LiteralValue(TestBoolean(true))(AtomicPositionInfo.None)
            ),
            None,Nil,None,Nil,None,None,None,Set()
          ),
          t(5),Some(ScopedResourceName(0,rn("bbbb-bbbb"))), Some(rn("bbbb-bbbb"))
        ),
        LiteralValue(TestBoolean(true))(AtomicPositionInfo.None)
      )
    )
  }

  test("logical positions - alias expansion") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select n1 + n2 as sum where sum = 5")

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    val lhs = select.where match {
      case Some(FunctionCall(_, Seq(fc@FunctionCall(_, _), _))) => fc
      case _ => fail("Expected a function call with exactly 2 args in the where, the left hand of which is another function call")
    }

    val pos = lhs.position

    pos.logicalPosition.longString must equal (
      """select n1 + n2 as sum where sum = 5
        |                            ^""".stripMargin
    )
    pos.physicalPosition.longString must equal (
      """select n1 + n2 as sum where sum = 5
        |       ^""".stripMargin
    )
    pos.functionNamePosition.longString must equal (
      """select n1 + n2 as sum where sum = 5
        |          ^""".stripMargin
    )
  }

  test("logical positions - udf parameters") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "bbbb-bbbb") -> U(0, "select 1 from @single-row where ?x = 5", "x" -> TestNumber)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select * join @bbbb-bbbb(7) on true")

    val select = analysis.statement match {
      case select: Select[Int, TestType, TestValue] => select
      case _ => fail("Expected a select")
    }

    val subquery = select.from match {
      case Join(_, _, _, FromStatement(subquery: Select[Int, TestType, TestValue], _, _, _), _) => subquery
      case _ => fail("Expected a join to a subquery")
    }

    val lit = subquery.from match {
      case Join(_, _, FromStatement(Values(NonEmptySeq(NonEmptySeq(lit@LiteralValue(_), Nil), Nil)), _, _, _), _, _) => lit
      case _ => fail("Expected a join to a values")
    }

    lit.position.physicalPosition.longString must equal(
      """select * join @bbbb-bbbb(7) on true
        |                         ^""".stripMargin
    )

    lit.position.logicalPosition.longString must equal(
      """select * join @bbbb-bbbb(7) on true
        |                         ^""".stripMargin
    )
  }

  test("Hidden columns - an unordered table") {
    val tf1 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber).withHiddenColumns("n2")
    )
    val analysis1 = analyzeSaved(tf1, "aaaa-aaaa")

    val tf2 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber)
    )
    val analysis2 = analyzeSaved(tf2, "aaaa-aaaa")

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Hidden columns - table with a hidden ordering") {
    val tf1 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber).withHiddenColumns("n2").withOrdering("n2")
    )
    val analysis1 = analyzeSaved(tf1, "aaaa-aaaa")

    val tf2 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber)
    )
    val analysis2 = analyze(tf2, "aaaa-aaaa", "select n1 order by n2").merge(TestFunctions.And.monomorphic.get)

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Hidden columns - within a query on a sorted table") {
    val tf1 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber).withHiddenColumns("n2").withOrdering("n2")
    )
    val analysis1 = analyze(tf1, "aaaa-aaaa", "select n1 * 5")

    val tf2 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber)
    )
    val analysis2 = analyze(tf2, "aaaa-aaaa", "select n1 order by n2 |> select n1 * 5")

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Hidden columns - within a UDF") {
    val tf1 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "udf") -> U(0, "select n1, n2 from @aaaa-aaaa where n2 = ?x", "x" -> TestNumber).withHiddenColumns("n2")
    )
    val analysis1 = analyze(tf1, "select @udf.* from @single_row join @udf(5) on true")

    val tf2 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "udf") -> U(0, "select n1 from @aaaa-aaaa where n2 = ?x", "x" -> TestNumber)
    )
    val analysis2 = analyze(tf2, "select @udf.n1 from @single_row join @udf(5) on true")

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Hidden columns - indistinct") {
    val tf1 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "q") -> Q(0, "aaaa-aaaa", "select n1, n2 order by n2 limit 5 offset 6").withHiddenColumns("n2")
    )
    val analysis1 = analyzeSaved(tf1, "q")

    val tf2 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "q") -> Q(0, "aaaa-aaaa", "select n1 order by n2 limit 5 offset 6")
    )
    val analysis2 = analyzeSaved(tf2, "q")

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Hidden columns - distinct on") {
    val tf1 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "q") -> Q(0, "aaaa-aaaa", "select distinct on(n2) n1, n2 order by n2").withHiddenColumns("n2")
    )
    val analysis1 = analyzeSaved(tf1, "q")

    val tf2 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "q") -> Q(0, "aaaa-aaaa", "select distinct on(n2) n1 order by n2")
    )
    val analysis2 = analyzeSaved(tf2, "q")

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Hidden columns - distinct") {
    val tf1 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "q") -> Q(0, "aaaa-aaaa", "select distinct n1, n2 order by n2 limit 5 offset 6").withHiddenColumns("n2")
    )
    val analysis1 = analyzeSaved(tf1, "q")

    val tf2 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "q") -> Q(0, "aaaa-aaaa", "select distinct n1, n2 |> select n1 order by n2 limit 5 offset 6")
    )
    val analysis2 = analyzeSaved(tf2, "q")

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Hidden columns - distinct with window functions") {
    val tf1 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "q") -> Q(0, "aaaa-aaaa", "select distinct n1, n2, row_number() over () as rn order by n2 limit 5 offset 6").withHiddenColumns("n2")
    )
    val analysis1 = analyzeSaved(tf1, "q")

    val tf2 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "q") -> Q(0, "aaaa-aaaa", "select distinct n1, n2, row_number() over () as rn order by n2 limit 5 offset 6 |> select n1, rn order by n2")
    )
    val analysis2 = analyzeSaved(tf2, "q")

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("fully distinct requires selected order by") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
    )

    expectFailure(0) {
      val analysis = analyze(tf, "aaaa-aaaa", "select distinct n1 order by n2")(new OnFail {
        override def onAnalyzerError(e: SoQLAnalyzerError[Int, SoQLAnalyzerError.AnalysisError]): Nothing = {
          e match {
            case SoQLAnalyzerError.TextualError(_, _, _, SoQLAnalyzerError.AnalysisError.OrderByMustBeSelectedWhenDistinct) =>
              expected(0)
            case _ =>
              super.onAnalyzerError(e)
          }
        }
      })
    }
  }

  test("preserve system columns - simple") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber)
    )

    val foundTables = tf.findTables(0, rn("twocol"), "select text + text, num * 2", Map.empty).toOption.get
    val analysis = systemColumnPreservingAnalyzer(foundTables, UserParameters.empty).toOption.get

    val expectedAnalysis = analyze(tf, "twocol", "select text + text, num * 2, :id")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve system columns - partially selected") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, ":version" -> TestNumber, "text" -> TestText, "num" -> TestNumber)
    )

    val foundTables = tf.findTables(0, rn("twocol"), "select :id, text + text, num * 2", Map.empty).toOption.get
    val analysis = systemColumnPreservingAnalyzer(foundTables, UserParameters.empty).toOption.get

    val expectedAnalysis = analyze(tf, "twocol", "select :id, text + text, num * 2, :version")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve system columns - distinct blocks") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, ":version" -> TestNumber, "text" -> TestText, "num" -> TestNumber)
    )

    val foundTables = tf.findTables(0, rn("twocol"), "select distinct text + text, num * 2", Map.empty).toOption.get
    val analysis = systemColumnPreservingAnalyzer(foundTables, UserParameters.empty).toOption.get

    val expectedAnalysis = analyze(tf, "twocol", "select distinct text + text, num * 2")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve system columns - table ops block") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, ":version" -> TestNumber, "text" -> TestText, "num" -> TestNumber)
    )

    val foundTables = tf.findTables(0, rn("twocol"), "(select * |> select *) union (select * from @twocol |> select *)", Map.empty).toOption.get
    val analysis = systemColumnPreservingAnalyzer(foundTables, UserParameters.empty).toOption.get

    val expectedAnalysis = analyze(tf, "twocol", "(select *, :id, :version |> select *) union (select *, :id, :version from @twocol |> select *)")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
  }

  test("preserve system columns - aggregating") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber)
    )

    val foundTables = tf.findTables(0, rn("twocol"), "select text, count(*) group by text", Map.empty).toOption.get
    val analysis = systemColumnPreservingAnalyzer(foundTables, UserParameters.empty).toOption.get

    val expectedAnalysis = analyze(tf, "twocol", "select text, count(*), max(:id) group by text")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
    analysis.statement.schema.find(_._2.name == cn(":id")) must not be empty
  }

  test("preserve system columns - udf") {
    val tf = tableFinder(
      (0, "twocol") -> D(":id" -> TestNumber, "text" -> TestText, "num" -> TestNumber),
      (0, "udf") -> U(0, "select num from @twocol where text = ?t", "t" -> TestText),
      (0, "udf2") -> U(0, "select num, :id from @twocol where text = ?t", "t" -> TestText)
    )

    val foundTables = tf.findTables(0, "select @udf.:*, @udf.* from @single_row join @udf('hello') on true", Map.empty).toOption.get
    val analysis = systemColumnPreservingAnalyzer(foundTables, UserParameters.empty).toOption.get

    val expectedAnalysis = analyze(tf, "select @udf2.:id, @udf2.num from @single_row join @udf2('hello') on true")

    analysis.statement must be (isomorphicTo(expectedAnalysis.statement))
    analysis.statement.schema.find(_._2.name == cn(":id")) must not be empty
  }

}
