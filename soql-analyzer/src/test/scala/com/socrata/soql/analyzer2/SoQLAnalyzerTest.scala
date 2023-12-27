package com.socrata.soql.analyzer2

import scala.util.parsing.input.{Position, NoPosition}

import java.math.{BigDecimal => JBigDecimal}

import org.scalatest.{FunSuite, MustMatchers}

import org.joda.time.{DateTime, LocalDate}

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ScopedResourceName, Source}
import com.socrata.soql.functions.MonomorphicFunction

import mocktablefinder._

class SoQLAnalyzerTest extends FunSuite with MustMatchers with TestHelper {
  object Pos {
    def unapply(pos: Position): Option[(Int, Int)] =
      pos match {
        case NoPosition => None
        case other => Some((other.line, other.column))
      }
  }

  test("alias analysis qualifiers are correct") {
    val tf = MockTableFinder.empty[TestMT]
    val Right(start) = tf.findTables(0, "select @bleh.whatever from @single_row", Map.empty)
    analyzer(start, UserParameters.empty) match {
      case Right(_) => fail("Expected an error")
      case Left(SoQLAnalyzerError.TypecheckError.NoSuchColumn(Source.Anonymous(Pos(1, 8)), qualifier, name)) =>
        qualifier must be (Some(rn("bleh")))
        name must be (cn("whatever"))
      case Left(err) =>
        fail(s"Wrong error: ${err}")
    }
  }

  test("simple contextless") {
    val tf = MockTableFinder.empty[TestMT]

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
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          FunctionCall[TestMT](
            MonomorphicFunction(TestFunctions.BinaryPlus, Map("a" -> TestNumber)),
            Seq(
              LiteralValue[TestMT](TestNumber(5))(AtomicPositionInfo.Synthetic),
              LiteralValue[TestMT](TestNumber(7))(AtomicPositionInfo.Synthetic)
            )
          )(FuncallPositionInfo.Synthetic),
          cn("_5_7"),
          false
        ),
        c(2) -> NamedExpr(
          FunctionCall[TestMT](
            MonomorphicFunction(TestFunctions.BinaryPlus, Map("a" -> TestText)),
            Seq(
              LiteralValue[TestMT](TestText("hello"))(AtomicPositionInfo.Synthetic),
              LiteralValue[TestMT](TestText("world"))(AtomicPositionInfo.Synthetic)
            )
          )(FuncallPositionInfo.Synthetic),
          cn("hello_world"),
          false
        ),
        c(3) -> NamedExpr(
          FunctionCall[TestMT](
            MonomorphicFunction(TestFunctions.BinaryPlus, Map("a" -> TestText)),
            Seq(
              LiteralValue[TestMT](TestText("1"))(AtomicPositionInfo.Synthetic),
              LiteralValue[TestMT](TestText("2"))(AtomicPositionInfo.Synthetic)
            )
          )(FuncallPositionInfo.Synthetic),
          cn("_1_2"),
          false
        )
      )
    )

    select.from must equal (FromSingleRow(t(1), Some(rn("single_row"))))

    // and the rest is empty
    val Select(Distinctiveness.Indistinct(), _, _, None, Nil, None, Nil, None, None, None, _) = select
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
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("text"), TestText)(AtomicPositionInfo.Synthetic),
          cn("t"),
          false
        ),
        c(2) -> NamedExpr(
          FunctionCall[TestMT](
            TestFunctions.Times.monomorphic.get,
            Seq(
              PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("num"), TestNumber)(AtomicPositionInfo.Synthetic),
              PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("num"), TestNumber)(AtomicPositionInfo.Synthetic)
            )
          )(FuncallPositionInfo.Synthetic),
          cn("num_num"),
          false
        )
      )
    )

    select.from must equal (
      FromTable[TestMT](
        dtn("aaaa-aaaa"),
        ScopedResourceName(0, rn("aaaa-aaaa")),
        None,
        t(1),
        OrderedMap(
          dcn("text") -> FromTable.ColumnInfo[TestMT](cn("text"), TestText, None),
          dcn("num") -> FromTable.ColumnInfo[TestMT](cn("num"), TestNumber, None)
        ),
        Nil
      )
    )

    // and the rest is empty
    val Select(Distinctiveness.Indistinct(), _, _, None, Nil, None, Nil, None, None, None, _) = select
  }

  test("untagged parameters in anonymous soql - impersonating a saved query") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(
      tf,
      "aaaa-aaaa",
      "select param('gnu')",
      canon("bbbb-bbbb"),
      UserParameters(
        qualified = Map(canon("bbbb-bbbb") -> Map(hn("gnu") -> UserParameters.Value(TestText("Hello world"))))
      )
    )

    val select = analysis.statement match {
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          LiteralValue[TestMT](TestText("Hello world"))(AtomicPositionInfo.Synthetic),
          cn("param_gnu"),
          false
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
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          LiteralValue[TestMT](TestText("Hello world"))(AtomicPositionInfo.Synthetic),
          cn("param_gnu"),
          false
        )
      )
    )
  }

  test("untagged parameters in anonymous soql - redirected parameters") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select param('gnu')", canon("bbbb-bbbb"),
      UserParameters(
        qualified = Map(canon("bbbb-bbbb") -> Map(hn("gnu") -> UserParameters.Value(TestText("Hello world"))))
      )
    )

    val select = analysis.statement match {
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(1) -> NamedExpr(
          LiteralValue[TestMT](TestText("Hello world"))(AtomicPositionInfo.Synthetic),
          cn("param_gnu"),
          false
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
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select must equal (
      Select[TestMT](
        Distinctiveness.Indistinct(),
        OrderedMap(
          c(1) -> NamedExpr(PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("text"), TestText)(AtomicPositionInfo.Synthetic), cn("text"), false),
          c(2) -> NamedExpr(PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("num"), TestNumber)(AtomicPositionInfo.Synthetic), cn("num"), false)
        ),
        FromTable[TestMT](
          dtn("aaaa-aaaa"),
          ScopedResourceName(0, rn("aaaa-aaaa")),
          None,
          t(1),
          OrderedMap(
            dcn("text") -> FromTable.ColumnInfo[TestMT](cn("text"), TestText, None),
            dcn("num") -> FromTable.ColumnInfo[TestMT](cn("num"), TestNumber, None)
          ),
          Nil
        ),
        None, Nil, None,
        List(
          OrderBy(PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("text"), TestText)(AtomicPositionInfo.Synthetic),true,true)
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
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select must equal (
      Select[TestMT](
        Distinctiveness.Indistinct(),
        OrderedMap(
          c(3) -> NamedExpr(VirtualColumn[TestMT](t(2),c(1),TestText)(AtomicPositionInfo.Synthetic),cn("text"),false),
          c(4) -> NamedExpr(VirtualColumn[TestMT](t(2),c(2),TestNumber)(AtomicPositionInfo.Synthetic),cn("num"),false)
        ),
        FromStatement[TestMT](
          Select[TestMT](
            Distinctiveness.Indistinct(),
            OrderedMap(
              c(1) -> NamedExpr(PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("text"), TestText)(AtomicPositionInfo.Synthetic), cn("text"),false),
              c(2) -> NamedExpr(PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("num"), TestNumber)(AtomicPositionInfo.Synthetic), cn("num"),false)
            ),
            FromTable[TestMT](
              dtn("aaaa-aaaa"),
              ScopedResourceName(0, rn("aaaa-aaaa")),
              None,
              t(1),
              OrderedMap(
                dcn("text") -> FromTable.ColumnInfo[TestMT](cn("text"), TestText, None),
                dcn("num") -> FromTable.ColumnInfo[TestMT](cn("num"), TestNumber, None)
              ),
              Nil
            ),
            None, Nil, None,
            List(
              OrderBy(PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("text"), TestText)(AtomicPositionInfo.Synthetic),true,true)
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
        override def onAnalyzerError(e: SoQLAnalyzerError[Int]): Nothing = {
          e match {
            case SoQLAnalyzerError.DistinctOnNotPrefixOfOrderBy(Source.Anonymous(Pos(1, 48))) =>
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
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(4) -> NamedExpr(
          PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("text"), TestText)(AtomicPositionInfo.Synthetic),
          cn("text"),
          false
        ),
        c(5) -> NamedExpr(
          PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("num"), TestNumber)(AtomicPositionInfo.Synthetic),
          cn("num"),
          false
        )
      )
    )

    select.from must equal (
      Join[TestMT](
        JoinType.Inner,
        true,
        FromTable[TestMT](
          dtn("aaaa-aaaa"), ScopedResourceName(0, rn("aaaa-aaaa")), None, t(1),
          OrderedMap(
            dcn("text") -> FromTable.ColumnInfo[TestMT](cn("text"), TestText, None),
            dcn("num") -> FromTable.ColumnInfo[TestMT](cn("num"), TestNumber, None)
          ),
          Nil
        ),
        FromStatement[TestMT](
          Select[TestMT](
            Distinctiveness.Indistinct(),
            OrderedMap(
              c(3) -> NamedExpr(VirtualColumn[TestMT](t(5),c(2),TestNumber)(AtomicPositionInfo.Synthetic), cn("_1"),false)
            ),
            Join[TestMT](
              JoinType.Inner,
              true,
              FromStatement[TestMT](
                Select[TestMT](
                  Distinctiveness.Indistinct(),
                  OrderedMap(c(1) -> NamedExpr(LiteralValue[TestMT](TestText("bob"))(AtomicPositionInfo.Synthetic), cn("user"), true)),
                  FromSingleRow(t(2), None),
                  None, Nil, None, Nil, None, None, None, Set.empty
                ),
                t(3), None, None
              ),
              FromStatement[TestMT](
                Select[TestMT](
                  Distinctiveness.Indistinct(),
                  OrderedMap(
                    c(2) -> NamedExpr(LiteralValue[TestMT](TestNumber(1))(AtomicPositionInfo.Synthetic),cn("_1"),false)
                  ),
                  FromTable[TestMT](
                    dtn("bbbb-bbbb"), ScopedResourceName(0, rn("bbbb-bbbb")),Some(rn("bbbb-bbbb")),
                    t(4),
                    OrderedMap(
                      dcn("user") -> FromTable.ColumnInfo[TestMT](cn("user"),TestText, None),
                      dcn("allowed") -> FromTable.ColumnInfo[TestMT](cn("allowed"),TestBoolean, None)
                    ),
                    Nil
                  ),
                  Some(
                    FunctionCall[TestMT](
                      TestFunctions.And.monomorphic.get,
                      Seq(
                        FunctionCall[TestMT](
                          MonomorphicFunction(TestFunctions.Eq, Map("a" -> TestText)),
                          Seq(
                            PhysicalColumn[TestMT](t(4),dtn("bbbb-bbbb"),dcn("user"),TestText)(AtomicPositionInfo.Synthetic),
                            VirtualColumn[TestMT](t(3),c(1),TestText)(AtomicPositionInfo.Synthetic)
                          )
                        )(FuncallPositionInfo.Synthetic),
                        PhysicalColumn[TestMT](t(4),dtn("bbbb-bbbb"),dcn("allowed"),TestBoolean)(AtomicPositionInfo.Synthetic)
                      )
                    )(FuncallPositionInfo.Synthetic)
                  ),
                  Nil,None,Nil,Some(1),None,None,Set.empty
                ),
                t(5), None, None
              ),
              LiteralValue[TestMT](TestBoolean(true))(AtomicPositionInfo.Synthetic)
            ),
            None,Nil,None,Nil,None,None,None,Set.empty
          ),
          t(6),
          Some(ScopedResourceName(0,rn("cccc-cccc"))),Some(rn("cccc-cccc"))
        ),
        LiteralValue[TestMT](TestBoolean(true))(AtomicPositionInfo.Synthetic)
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
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(4) -> NamedExpr(
          PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("text"), TestText)(AtomicPositionInfo.Synthetic),
          cn("text"),
          false
        ),
        c(5) -> NamedExpr(
          PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("num"), TestNumber)(AtomicPositionInfo.Synthetic),
          cn("num"),
          false
        )
      )
    )

    select.from must equal (
      Join[TestMT](
        JoinType.Inner,
        true,
        FromTable[TestMT](
          dtn("aaaa-aaaa"), ScopedResourceName(0, rn("aaaa-aaaa")), None, t(1),
          OrderedMap(
            dcn("text") -> FromTable.ColumnInfo[TestMT](cn("text"), TestText, None),
            dcn("num") -> FromTable.ColumnInfo[TestMT](cn("num"), TestNumber, None)
          ),
          Nil
        ),
        FromStatement[TestMT](
          Select[TestMT](
            Distinctiveness.Indistinct(),
            OrderedMap(
              c(3) -> NamedExpr(VirtualColumn[TestMT](t(5),c(2),TestNumber)(AtomicPositionInfo.Synthetic), cn("_1"),false)
            ),
            Join[TestMT](
              JoinType.Inner,
              true,
              FromStatement(
                Select[TestMT](
                  Distinctiveness.Indistinct(),
                  OrderedMap(c(1) -> NamedExpr(PhysicalColumn[TestMT](t(1),dtn("aaaa-aaaa"),dcn("text"),TestText)(AtomicPositionInfo.Synthetic), cn("user"), true)),
                  FromSingleRow(t(2), None),
                  None, Nil, None, Nil, None, None, None, Set.empty
                ),
                t(3), None, None
              ),
              FromStatement[TestMT](
                Select[TestMT](
                  Distinctiveness.Indistinct(),
                  OrderedMap(
                    c(2) -> NamedExpr(LiteralValue[TestMT](TestNumber(1))(AtomicPositionInfo.Synthetic),cn("_1"),false)
                  ),
                  FromTable[TestMT](
                    dtn("bbbb-bbbb"), ScopedResourceName(0, rn("bbbb-bbbb")),Some(rn("bbbb-bbbb")),
                    t(4),
                    OrderedMap(
                      dcn("user") -> FromTable.ColumnInfo[TestMT](cn("user"),TestText, None),
                      dcn("allowed") -> FromTable.ColumnInfo[TestMT](cn("allowed"),TestBoolean, None)
                    ),
                    Nil
                  ),
                  Some(
                    FunctionCall[TestMT](
                      TestFunctions.And.monomorphic.get,
                      Seq(
                        FunctionCall[TestMT](
                          MonomorphicFunction(TestFunctions.Eq, Map("a" -> TestText)),
                          Seq(
                            PhysicalColumn[TestMT](t(4),dtn("bbbb-bbbb"),dcn("user"),TestText)(AtomicPositionInfo.Synthetic),
                            VirtualColumn[TestMT](t(3),c(1),TestText)(AtomicPositionInfo.Synthetic)
                          )
                        )(FuncallPositionInfo.Synthetic),
                        PhysicalColumn[TestMT](t(4),dtn("bbbb-bbbb"),dcn("allowed"),TestBoolean)(AtomicPositionInfo.Synthetic)
                      )
                    )(FuncallPositionInfo.Synthetic)
                  ),
                  Nil,None,Nil,Some(1),None,None,Set.empty
                ),
                t(5), None, None
              ),
              LiteralValue[TestMT](TestBoolean(true))(AtomicPositionInfo.Synthetic)
            ),
            None,Nil,None,Nil,None,None,None,Set.empty
          ),
          t(6),
          Some(ScopedResourceName(0,rn("cccc-cccc"))),Some(rn("cccc-cccc"))
        ),
        LiteralValue[TestMT](TestBoolean(true))(AtomicPositionInfo.Synthetic)
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
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    select.selectList must equal (
      OrderedMap(
        c(13) -> NamedExpr(
          PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("text"), TestText)(AtomicPositionInfo.Synthetic),
          cn("text"),
          false
        ),
        c(14) -> NamedExpr(
          PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("num"), TestNumber)(AtomicPositionInfo.Synthetic),
          cn("num"),
          false
        )
      )
    )

    select.from must equal (
      Join[TestMT](
        JoinType.Inner,
        true,
        FromTable[TestMT](
          dtn("aaaa-aaaa"), ScopedResourceName(0, rn("aaaa-aaaa")), None, t(1),
          OrderedMap(
            dcn("text") -> FromTable.ColumnInfo[TestMT](cn("text"), TestText, None),
            dcn("num") -> FromTable.ColumnInfo[TestMT](cn("num"), TestNumber, None)
          ),
          Nil
        ),
        FromStatement[TestMT](
          Select[TestMT](
            Distinctiveness.Indistinct(),
            OrderedMap(
              c(9) -> NamedExpr(VirtualColumn[TestMT](t(5), c(5), TestText)(AtomicPositionInfo.Synthetic), cn("a"), false),
              c(10) -> NamedExpr(VirtualColumn[TestMT](t(5), c(6), TestBoolean)(AtomicPositionInfo.Synthetic), cn("b"), false),
              c(11) -> NamedExpr(VirtualColumn[TestMT](t(5), c(7), TestNumber)(AtomicPositionInfo.Synthetic), cn("c"), false),
              c(12) -> NamedExpr(VirtualColumn[TestMT](t(5), c(8), TestText)(AtomicPositionInfo.Synthetic), cn("d"), false)
            ),
            Join[TestMT](
              JoinType.Inner,
              true,
              FromStatement[TestMT](
                Select[TestMT](
                  Distinctiveness.Indistinct(),
                  OrderedMap(
                    c(1) -> NamedExpr(
                      FunctionCall[TestMT](
                        TestFunctions.castIdentitiesByType(TestText).monomorphic.get,
                        Seq(LiteralValue[TestMT](TestText("hello"))(AtomicPositionInfo.Synthetic))
                      )(FuncallPositionInfo.Synthetic),
                      cn("d"),
                      true
                    ),
                    c(2) -> NamedExpr(
                      LiteralValue[TestMT](TestNumber(5))(AtomicPositionInfo.Synthetic),
                      cn("c"),
                      true
                    ),
                    c(3) -> NamedExpr(
                      FunctionCall[TestMT](
                        TestFunctions.castIdentitiesByType(TestBoolean).monomorphic.get,
                        Seq(LiteralValue[TestMT](TestBoolean(true))(AtomicPositionInfo.Synthetic))
                      )(FuncallPositionInfo.Synthetic),
                      cn("b"),
                      true
                    ),
                    c(4) -> NamedExpr(
                      PhysicalColumn[TestMT](t(1), dtn("aaaa-aaaa"), dcn("text"), TestText)(AtomicPositionInfo.Synthetic),
                      cn("a"),
                      true
                    )
                  ),
                  FromSingleRow(t(2), None),
                  None, Nil, None, Nil, None, None, None, Set.empty
                ),
                t(3), None, None
              ),
              FromStatement(
                Select[TestMT](
                  Distinctiveness.Indistinct(),
                  OrderedMap(
                    c(5) -> NamedExpr(VirtualColumn[TestMT](t(3),c(4),TestText)(AtomicPositionInfo.Synthetic), cn("a"), false),
                    c(6) -> NamedExpr(VirtualColumn[TestMT](t(3),c(3),TestBoolean)(AtomicPositionInfo.Synthetic), cn("b"), false),
                    c(7) -> NamedExpr(VirtualColumn[TestMT](t(3),c(2),TestNumber)(AtomicPositionInfo.Synthetic), cn("c"), false),
                    c(8) -> NamedExpr(VirtualColumn[TestMT](t(3),c(1),TestText)(AtomicPositionInfo.Synthetic), cn("d"), false)
                  ),
                  FromSingleRow(t(4), Some(rn("single_row"))),
                  None,Nil,None,Nil,None,None,None,Set()
                ),
                t(5), None, None
              ),
              LiteralValue[TestMT](TestBoolean(true))(AtomicPositionInfo.Synthetic)
            ),
            None,Nil,None,Nil,None,None,None,Set()
          ),
          t(6),Some(ScopedResourceName(0,rn("bbbb-bbbb"))), Some(rn("bbbb-bbbb"))
        ),
        LiteralValue[TestMT](TestBoolean(true))(AtomicPositionInfo.Synthetic)
      )
    )
  }

  test("logical positions - alias expansion") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber)
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select n1 + n2 as sum where sum = 5")

    val select = analysis.statement match {
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    val lhs = select.where match {
      case Some(FunctionCall(_, Seq(fc@FunctionCall(_, _), _))) => fc
      case _ => fail("Expected a function call with exactly 2 args in the where, the left hand of which is another function call")
    }

    val pos = lhs.position

    pos.reference.position.longString must equal (
      """select n1 + n2 as sum where sum = 5
        |                            ^""".stripMargin
    )
    pos.source.position.longString must equal (
      """select n1 + n2 as sum where sum = 5
        |       ^""".stripMargin
    )
    pos.functionNameSource.position.longString must equal (
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
      case select: Select[TestMT] => select
      case _ => fail("Expected a select")
    }

    val subquery = select.from match {
      case Join(_, _, _, FromStatement(subquery: Select[TestMT], _, _, _), _) => subquery
      case _ => fail("Expected a join to a subquery")
    }

    val lit = subquery.from match {
      case Join(_, _, FromStatement(Select(_, map, FromSingleRow(_, _), _, _, _, _, _, _, _, _), _, _, _), _, _) if map.size == 1 =>
        map.head._2.expr
      case _ => fail("Expected a join to a single-row subselect")
    }

    lit.position.source.position.longString must equal(
      """select * join @bbbb-bbbb(7) on true
        |                         ^""".stripMargin
    )

    lit.position.reference.position.longString must equal(
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
      (0, "q") -> Q(0, "aaaa-aaaa", "select distinct n1, n2, n1 + n2 order by n2 limit 5 offset 6").withHiddenColumns("n2")
    )
    val analysis1 = analyzeSaved(tf1, "q")

    val tf2 = tableFinder(
      (0, "aaaa-aaaa") -> D("n1" -> TestNumber, "n2" -> TestNumber),
      (0, "q") -> Q(0, "aaaa-aaaa", "select distinct n1, n2, n1 + n2 |> select n1, n1_n2 order by n2 limit 5 offset 6")
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
        override def onAnalyzerError(e: SoQLAnalyzerError[Int]): Nothing = {
          e match {
            case SoQLAnalyzerError.OrderByMustBeSelectedWhenDistinct(Source.Anonymous(Pos(1, 29))) =>
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

  test("case expression gets rewritten to case function") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber).withOrdering("text")
    )

    val analysis = analyze(tf, "aaaa-aaaa", "select case when text = 'hello' then num else -num end")
    val analysis2 = analyze(tf, "aaaa-aaaa", "select case(text = 'hello', num, true, -num)")

    analysis.statement must be (isomorphicTo(analysis2.statement))
  }

  test("cannot use OVER with count(distinct)") {
    val tf = tableFinder(
      (0, "aaaa-aaaa") -> D("text" -> TestText, "num" -> TestNumber).withOrdering("text")
    )

    val Right(start) = tf.findTables(0, rn("aaaa-aaaa"), "select count(distinct text) over (partition by num)", Map.empty)
    analyzer(start, UserParameters.empty) match {
      case Right(_) => fail("Expected an error")
      case Left(SoQLAnalyzerError.TypecheckError.DistinctWithOver(Source.Anonymous(Pos(1, 8)))) =>
        // yay
      case Left(err) =>
        fail(s"Wrong error: ${err}")
    }
  }

  test("Can use @this on the RHS of a table op") {
    val tf = tableFinder(
      (0, "ds") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "q") -> Q(0, "ds", "select text as t, num*2 as n"),
      (0, "w") -> Q(0, "ds", "select text + text as tt, num*3 as n3")
    )

    val Right(ft1) = tf.findTables(0, rn("q"), "(select tt, n3 from @w) union (select @t.t, @t.n from @this as @t)", Map.empty)
    val Right(analysis1) = analyzer(ft1, UserParameters.empty)
    val Right(ft2) = tf.findTables(0, "(select tt, n3 from @w) union (select t, n from @q)", Map.empty)
    val Right(analysis2) = analyzer(ft2, UserParameters.empty)

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Can use nothing on the RHS of a table op") {
    val tf = tableFinder(
      (0, "ds") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "q") -> Q(0, "ds", "select text as t, num*2 as n"),
      (0, "w") -> Q(0, "ds", "select text + text as tt, num*3 as n3")
    )

    val Right(ft1) = tf.findTables(0, rn("q"), "(select tt, n3 from @w) union (select t, n)", Map.empty)
    val Right(analysis1) = analyzer(ft1, UserParameters.empty)
    val Right(ft2) = tf.findTables(0, "(select tt, n3 from @w) union (select t, n from @q)", Map.empty)
    val Right(analysis2) = analyzer(ft2, UserParameters.empty)

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Can use @this on the LHS of a table op") {
    val tf = tableFinder(
      (0, "ds") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "q") -> Q(0, "ds", "select text as t, num*2 as n"),
      (0, "w") -> Q(0, "ds", "select text + text as tt, num*3 as n3")
    )

    val Right(ft1) = tf.findTables(0, rn("q"), "(select @t.t, @t.n from @this as @t) union (select tt, n3 from @w)", Map.empty)
    val Right(analysis1) = analyzer(ft1, UserParameters.empty)
    val Right(ft2) = tf.findTables(0, "(select t, n from @q) union (select tt, n3 from @w)", Map.empty)
    val Right(analysis2) = analyzer(ft2, UserParameters.empty)

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Can use nothing on the LHS of a table op") {
    val tf = tableFinder(
      (0, "ds") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "q") -> Q(0, "ds", "select text as t, num*2 as n"),
      (0, "w") -> Q(0, "ds", "select text + text as tt, num*3 as n3")
    )

    val Right(ft1) = tf.findTables(0, rn("q"), "(select t, n) union (select tt, n3 from @w)", Map.empty)
    val Right(analysis1) = analyzer(ft1, UserParameters.empty)
    val Right(ft2) = tf.findTables(0, "(select t, n from @q) union (select tt, n3 from @w)", Map.empty)
    val Right(analysis2) = analyzer(ft2, UserParameters.empty)

    analysis1.statement must be (isomorphicTo(analysis2.statement))
  }

  test("Must use the context _somewhere_ in a contextual table op") {
    val tf = tableFinder(
      (0, "ds") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "q") -> Q(0, "ds", "select text as t, num*2 as n"),
      (0, "w") -> Q(0, "ds", "select text + text as tt, num*3 as n3")
    )

    val Right(ft) = tf.findTables(0, rn("q"), "(select t, n from @q) union (select tt, n3 from @w)", Map.empty)
    val Left(SoQLAnalyzerError.FromForbidden(_)) = analyzer(ft, UserParameters.empty)
  }
}
