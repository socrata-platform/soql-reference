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
    val tf.Success(start) = tf.findTables(0, "select @bleh.whatever from @single_row", Map.empty)
    analyzer(start, UserParameters.empty) match {
      case Right(_) => fail("Expected an error")
      case Left(nsc: SoQLAnalyzerError.TypecheckError.NoSuchColumn[_]) =>
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
              LiteralValue(TestNumber(5))(NoPosition),
              LiteralValue(TestNumber(7))(NoPosition)
            )
          )(NoPosition, NoPosition),
          cn("_5_7")
        ),
        c(2) -> NamedExpr(
          FunctionCall(
            MonomorphicFunction(TestFunctions.BinaryPlus, Map("a" -> TestText)),
            Seq(
              LiteralValue(TestText("hello"))(NoPosition),
              LiteralValue(TestText("world"))(NoPosition)
            )
          )(NoPosition, NoPosition),
          cn("hello_world")
        ),
        c(3) -> NamedExpr(
          FunctionCall(
            MonomorphicFunction(TestFunctions.BinaryPlus, Map("a" -> TestText)),
            Seq(
              LiteralValue(TestText("1"))(NoPosition),
              LiteralValue(TestText("2"))(NoPosition)
            )
          )(NoPosition, NoPosition),
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
          Column(t(1), dcn("text"), TestText)(NoPosition),
          cn("t")
        ),
        c(2) -> NamedExpr(
          FunctionCall(
            TestFunctions.Times.monomorphic.get,
            Seq(
              Column(t(1), dcn("num"), TestNumber)(NoPosition),
              Column(t(1), dcn("num"), TestNumber)(NoPosition)
            )
          )(NoPosition, NoPosition),
          cn("num_num")
        )
      )
    )

    select.from must equal (
      FromTable(
        dtn("aaaa-aaaa"),
        QualifiedResourceName(0, rn("aaaa-aaaa")),
        None,
        t(1),
        OrderedMap(
          dcn("text") -> NameEntry(cn("text"), TestText),
          dcn("num") -> NameEntry(cn("num"), TestNumber)
        )
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
          LiteralValue(TestText("Hello world"))(NoPosition),
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
          LiteralValue(TestText("Hello world"))(NoPosition),
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
          LiteralValue(TestText("Hello world"))(NoPosition),
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
          c(1) -> NamedExpr(Column(t(1), dcn("text"), TestText)(NoPosition), cn("text")),
          c(2) -> NamedExpr(Column(t(1), dcn("num"), TestNumber)(NoPosition), cn("num"))
        ),
        FromTable(
          dtn("aaaa-aaaa"),
          QualifiedResourceName(0, rn("aaaa-aaaa")),
          None,
          t(1),
          OrderedMap(
            dcn("text") -> NameEntry(cn("text"), TestText),
            dcn("num") -> NameEntry(cn("num"), TestNumber)
          )
        ),
        None, Nil, None,
        List(
          OrderBy(Column(t(1), dcn("text"), TestText)(NoPosition),true,true)
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
          c(3) -> NamedExpr(Column(t(2),c(1),TestText)(NoPosition),cn("text")),
          c(4) -> NamedExpr(Column(t(2),c(2),TestNumber)(NoPosition),cn("num"))
        ),
        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            OrderedMap(
              c(1) -> NamedExpr(Column(t(1), dcn("text"), TestText)(NoPosition), cn("text")),
              c(2) -> NamedExpr(Column(t(1), dcn("num"), TestNumber)(NoPosition), cn("num"))
            ),
            FromTable(
              dtn("aaaa-aaaa"),
              QualifiedResourceName(0, rn("aaaa-aaaa")),
              None,
              t(1),
              OrderedMap(
                dcn("text") -> NameEntry(cn("text"), TestText),
                dcn("num") -> NameEntry(cn("num"), TestNumber)
              )
            ),
            None, Nil, None,
            List(
              OrderBy(Column(t(1), dcn("text"), TestText)(NoPosition),true,true)
            ),
            None,None,None,Set.empty
          ),
          t(2),
          Some(QualifiedResourceName(0, rn("aaaa-aaaa"))),
          None
        ),
        None,Nil,None,Nil,None,None,None,Set.empty
      )
    )
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
        c(2) -> NamedExpr(
          Column(t(1), dcn("text"), TestText)(NoPosition),
          cn("text")
        ),
        c(3) -> NamedExpr(
          Column(t(1), dcn("num"), TestNumber)(NoPosition),
          cn("num")
        )
      )
    )

    select.from must equal (
      Join(
        JoinType.Inner,
        true,
        FromTable(
          dtn("aaaa-aaaa"), QualifiedResourceName(0, rn("aaaa-aaaa")), None, t(1),
          OrderedMap(
            dcn("text") -> NameEntry(cn("text"), TestText),
            dcn("num") -> NameEntry(cn("num"), TestNumber)
          )
        ),
        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            OrderedMap(c(1) -> NamedExpr(LiteralValue(TestNumber(1))(NoPosition), cn("_1"))),
            FromTable(
              dtn("bbbb-bbbb"),
              QualifiedResourceName(0,rn("bbbb-bbbb")),
              Some(rn("bbbb-bbbb")),
              t(2),
              OrderedMap(
                dcn("user") -> NameEntry(cn("user"), TestText),
                dcn("allowed") -> NameEntry(cn("allowed"), TestBoolean)
              )
            ),
            Some(
              FunctionCall(
                TestFunctions.And.monomorphic.get,
                Seq(
                  FunctionCall(
                    MonomorphicFunction(TestFunctions.Eq, Map("a" -> TestText)),
                    Seq(
                      Column(t(2),dcn("user"),TestText)(NoPosition),
                      LiteralValue(TestText("bob"))(NoPosition)
                    )
                  )(NoPosition, NoPosition),
                  Column(t(2),dcn("allowed"),TestBoolean)(NoPosition)
                )
              )(NoPosition, NoPosition)
            ),
            Nil, None, Nil, Some(1), None, None, Set.empty
          ),
          t(3),
          Some(QualifiedResourceName(0, rn("cccc-cccc"))),
          Some(rn("cccc-cccc"))
        ),
        LiteralValue(TestBoolean(true))(NoPosition)
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
        c(2) -> NamedExpr(
          Column(t(1), dcn("text"), TestText)(NoPosition),
          cn("text")
        ),
        c(3) -> NamedExpr(
          Column(t(1), dcn("num"), TestNumber)(NoPosition),
          cn("num")
        )
      )
    )

    select.from must equal (
      Join(
        JoinType.Inner,
        true,
        FromTable(
          dtn("aaaa-aaaa"), QualifiedResourceName(0, rn("aaaa-aaaa")), None, t(1),
          OrderedMap(
            dcn("text") -> NameEntry(cn("text"), TestText),
            dcn("num") -> NameEntry(cn("num"), TestNumber)
          )
        ),
        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            OrderedMap(c(1) -> NamedExpr(LiteralValue(TestNumber(1))(NoPosition), cn("_1"))),
            FromTable(
              dtn("bbbb-bbbb"),
              QualifiedResourceName(0,rn("bbbb-bbbb")),
              Some(rn("bbbb-bbbb")),
              t(2),
              OrderedMap(
                dcn("user") -> NameEntry(cn("user"), TestText),
                dcn("allowed") -> NameEntry(cn("allowed"), TestBoolean)
              )
            ),
            Some(
              FunctionCall(
                TestFunctions.And.monomorphic.get,
                Seq(
                  FunctionCall(
                    MonomorphicFunction(TestFunctions.Eq, Map("a" -> TestText)),
                    Seq(
                      Column(t(2),dcn("user"),TestText)(NoPosition),
                      Column(t(1),dcn("text"),TestText)(NoPosition)
                    )
                  )(NoPosition, NoPosition),
                  Column(t(2),dcn("allowed"),TestBoolean)(NoPosition)
                )
              )(NoPosition, NoPosition)
            ),
            Nil, None, Nil, Some(1), None, None, Set.empty
          ),
          t(3),
          Some(QualifiedResourceName(0, rn("cccc-cccc"))),
          Some(rn("cccc-cccc"))
        ),
        LiteralValue(TestBoolean(true))(NoPosition)
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
          Column(t(1), dcn("text"), TestText)(NoPosition),
          cn("text")
        ),
        c(10) -> NamedExpr(
          Column(t(1), dcn("num"), TestNumber)(NoPosition),
          cn("num")
        )
      )
    )

    select.from must equal (
      Join(
        JoinType.Inner,
        true,
        FromTable(
          dtn("aaaa-aaaa"), QualifiedResourceName(0, rn("aaaa-aaaa")), None, t(1),
          OrderedMap(
            dcn("text") -> NameEntry(cn("text"), TestText),
            dcn("num") -> NameEntry(cn("num"), TestNumber)
          )
        ),
        FromStatement(
          Select(
            Distinctiveness.Indistinct,
            OrderedMap(
              c(5) -> NamedExpr(Column(t(4), c(1), TestText)(NoPosition), cn("a")),
              c(6) -> NamedExpr(Column(t(4), c(2), TestBoolean)(NoPosition), cn("b")),
              c(7) -> NamedExpr(Column(t(4), c(3), TestNumber)(NoPosition), cn("c")),
              c(8) -> NamedExpr(Column(t(4), c(4), TestText)(NoPosition), cn("d"))
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
                        Seq(LiteralValue(TestText("hello"))(NoPosition))
                      )(NoPosition, NoPosition),
                      Seq(
                        FunctionCall(
                          TestFunctions.castIdentitiesByType(TestBoolean).monomorphic.get,
                          Seq(LiteralValue(TestBoolean(true))(NoPosition))
                        )(NoPosition, NoPosition)
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
                    c(1) -> NamedExpr(Column(t(1),dcn("text"),TestText)(NoPosition), cn("a")),
                    c(2) -> NamedExpr(Column(t(2),dcn("column2"),TestBoolean)(NoPosition), cn("b")),
                    c(3) -> NamedExpr(LiteralValue(TestNumber(5))(NoPosition), cn("c")),
                    c(4) -> NamedExpr(Column(t(2),dcn("column1"),TestText)(NoPosition), cn("d"))
                  ),
                  FromSingleRow(t(3), Some(rn("single_row"))),
                  None,Nil,None,Nil,None,None,None,Set()
                ),
                t(4), None, None
              ),
              LiteralValue(TestBoolean(true))(NoPosition)
            ),
            None,Nil,None,Nil,None,None,None,Set()
          ),
          t(5),Some(QualifiedResourceName(0,rn("bbbb-bbbb"))), Some(rn("bbbb-bbbb"))
        ),
        LiteralValue(TestBoolean(true))(NoPosition)
      )
    )
  }
}
