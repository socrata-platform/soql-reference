package com.socrata.soql.analyzer2

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import org.scalatest.FunSuite
import org.scalatest.MustMatchers

import com.socrata.soql.BinaryTree
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.{StandaloneParser, AbstractParser}

import mocktablefinder._

class TableFinderTest extends FunSuite with MustMatchers {
  val tables = new MockTableFinder(
    Map(
      (0, "t1") -> D(
        "key" -> "integer",
        "value" -> "thing"
      ),
      (0, "t2") -> D(
        "key" -> "integer",
        "value" -> "otherthing"
      ),
      (0, "t3") -> Q(0, "t2", "select *"),
      (0, "t4") -> U(0, "select * from @t2"),
      (0, "t5") -> Q(1, "t1", "select *"),
      (0, "t6") -> Q(1, "t2", "select *"), // t2 exists in scope 0 but not in scope 1
      (0, "bad_one") -> Q(0, "bad_two", "select *"),
      (0, "bad_two") -> Q(0, "bad_one", "select *"),
      (0, "bad_three") -> Q(0, "t1", "select * join @bad_four on true"),
      (0, "bad_four") -> Q(0, "t1", "select * join @bad_three on true"),
      (0, "bad_five") -> Q(0, "t1", "select * join @bad_six on true"),
      (0, "bad_six") -> Q(0, "bad_five", "select *"),
      (1, "t1") -> D()
    )
  )

  def notFound(scope: Int, name: String) =
    tables.Error.NotFound(ScopedResourceName(scope, ResourceName(name)))

  test("can find a table") {
    tables.findTables(0, "select * from @t1", Map.empty).map(_.tableMap) must equal (tables((0, "t1")))
  }

  test("will reject mutually recursive queries - context") {
    tables.findTables(0, ResourceName("bad_one")) must equal (tables.Error.RecursiveQuery(List("bad_one","bad_two","bad_one").map(CanonicalName)))
  }

  test("will reject mutually recursive queries - from") {
    tables.findTables(0, ResourceName("bad_three")) must equal (tables.Error.RecursiveQuery(List("bad_three","bad_four","bad_three").map(CanonicalName)))
  }

  test("will reject mutually recursive queries - mixed") {
    tables.findTables(0, ResourceName("bad_five")) must equal (tables.Error.RecursiveQuery(List("bad_five","bad_six","bad_five").map(CanonicalName)))
  }

  test("can fail to find a table") {
    tables.findTables(0, "select * from @doesnt-exist", Map.empty).map(_.tableMap) must equal (notFound(0, "doesnt-exist"))
  }

  test("can find a table implicitly") {
    tables.findTables(0, ResourceName("t1"), "select key, value", Map.empty).map(_.tableMap) must equal (tables((0, "t1")))
  }

  test("can find a joined table") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t2 on @t2.key = key", Map.empty).map(_.tableMap) must equal (tables((0, "t1"), (0, "t2")))
  }

  test("can find a query that accesses another table") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t3 on @t3.key = key", Map.empty).map(_.tableMap) must equal (tables((0, "t1"), (0, "t2"), (0, "t3")))
  }

  test("can find a query that accesses another table via udf") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t4(1,2,3) on @t4.key = key", Map.empty).map(_.tableMap) must equal (tables((0, "t1"), (0, "t2"), (0, "t4")))
  }

  test("can find a in a different scope") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t5 on @t5.key = key", Map.empty).map(_.tableMap) must equal (tables((0, "t1"), (0, "t5"), (1, "t1")))
  }

  test("can fail to find a query in a different scope") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t6 on @t6.key = key", Map.empty).map(_.tableMap) must equal (notFound(1, "t2"))
  }

  test("can rountdtrip a FoundTables through JSON") {
    def go(ft: tables.Result[FoundTables[Int, String]]): Unit = {
      val orig = ft.asInstanceOf[tables.Success[FoundTables[Int, String]]].value
      val jsonized = JsonEncode.toJValue(orig)

      // first, can bounce it through UnparsedFoundTables and get the same JSON back out
      val unparsed = JsonDecode.fromJValue[UnparsedFoundTables[Int, String]](jsonized) match {
        case Right(ok) => ok
        case Left(err) => fail(err.english)
      }
      JsonEncode.toJValue(unparsed) must equal (jsonized)

      // Then, we can take that json and get the original FoundTables back out
      val parsed = JsonDecode.fromJValue[FoundTables[Int, String]](jsonized) match {
        case Right(ok) => ok
        case Left(err) => fail(err.english)
      }
      parsed must equal(orig)
    }

    go(tables.findTables(0, "select * from @t1", Map.empty))
    go(tables.findTables(0, ResourceName("t1"), "select key, value", Map.empty))
    go(tables.findTables(0, ResourceName("t1"), "select key, value join @t2 on @t2.key = key", Map.empty))
    go(tables.findTables(0, ResourceName("t1"), "select key, value join @t3 on @t3.key = key", Map.empty))
    go(tables.findTables(0, ResourceName("t1"), "select key, value join @t4(1,2,3) on @t4.key = key", Map.empty))
    go(tables.findTables(0, ResourceName("t1"), "select key, value join @t5 on @t5.key = key", Map.empty))
  }
}
