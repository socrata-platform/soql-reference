package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatest.matchers.{BeMatcher, MatchResult}

import com.socrata.soql.BinaryTree
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.{StandaloneParser, AbstractParser}

import mocktablefinder._

class TableFinderTest extends FunSuite with MustMatchers {
  final class MT extends MetaTypes {
    type ResourceNameScope = Int
    type ColumnType = String
    type ColumnValue = Nothing
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
  }

  val tables = new MockTableFinder[MT](
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
      (1, "t1") -> D(),

      (0, "graph_root") -> Q(0, "graph_parent_1", "select * join @graph_parent_2 on true"),
      (0, "graph_parent_1") -> Q(0, "graph_dataset_1", "select *"),
      (0, "graph_parent_2") -> Q(0, "graph_dataset_2", "select * join @graph_grandparent(1) on true"),
      (0, "graph_grandparent") -> U(0, "select * from @graph_dataset_3 join @graph_dataset_4 on true"),
      (0, "graph_dataset_1") -> D(),
      (0, "graph_dataset_2") -> D(),
      (0, "graph_dataset_3") -> D(),
      (0, "graph_dataset_4") -> D()
    )
  )

  class NotFoundMatcher(scope: tables.ResourceNameScope, name: ResourceName) extends BeMatcher[tables.Result[Any]] {
    def apply(err: tables.Result[Any]) =
      MatchResult(
        err match {
          case Left(SoQLAnalyzerError.TextualError(`scope`, _, _, SoQLAnalyzerError.TableFinderError.NotFound(nf))) =>
            nf == name
          case _ =>
            false
        },
        s"$err was not a not found error over ($scope, ${JString(name.name)})",
        s"$err was a not found error over ($scope, ${JString(name.name)})"
      )
  }
  def notFound(scope: tables.ResourceNameScope, name: String) =
    new NotFoundMatcher(scope, ResourceName(name))

  class RecursiveQueryMatcher[RNS](names: Seq[CanonicalName]) extends BeMatcher[tables.Result[Any]] {
    def apply(err: tables.Result[Any]) =
      MatchResult(
        err match {
          case Left(SoQLAnalyzerError.TextualError(_, _, _, SoQLAnalyzerError.TableFinderError.RecursiveQuery(path))) =>
            path == names
          case _ =>
            false
        },
        s"$err was not a recursive query error over ${names.map{n => JString(n.name)}.mkString(",")}",
        s"$err was a recursive query error over ${names.map{n => JString(n.name)}.mkString(",")}"
      )
  }
  def recursiveQuery(names: String*) =
    new RecursiveQueryMatcher(names.map(CanonicalName(_)))

  test("can find a table") {
    tables.findTables(0, "select * from @t1", Map.empty).map(_.tableMap) must equal (tables((0, "t1")))
  }

  test("will reject mutually recursive queries - context") {
    tables.findTables(0, ResourceName("bad_one")) must be (recursiveQuery("bad_one","bad_two","bad_one"))
  }

  test("will reject mutually recursive queries - from") {
    tables.findTables(0, ResourceName("bad_three")) must be (recursiveQuery("bad_three","bad_four","bad_three"))
  }

  test("will reject mutually recursive queries - mixed") {
    tables.findTables(0, ResourceName("bad_five")) must be (recursiveQuery("bad_five","bad_six","bad_five"))
  }

  test("can fail to find a table") {
    tables.findTables(0, "select * from @doesnt-exist", Map.empty).map(_.tableMap) must be (notFound(0, "doesnt-exist"))
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
    tables.findTables(0, ResourceName("t1"), "select key, value join @t6 on @t6.key = key", Map.empty).map(_.tableMap) must be (notFound(1, "t2"))
  }

  test("Can produce a graph from looking up a saved query") {
    def rn(s: String) = ResourceName(s)
    def srn(s: String) = ScopedResourceName(0, rn(s))

    val Right(foundTables) = tables.findTables(0, rn("graph_root"))
    val graph = foundTables.queryGraph

    graph.root must equal (Some(srn("graph_root")))
    graph.graph must equal (
      Map(
        Some(srn("graph_root")) -> Set(srn("graph_parent_1"), srn("graph_parent_2")),
        Some(srn("graph_parent_1")) -> Set(srn("graph_dataset_1")),
        Some(srn("graph_parent_2")) -> Set(srn("graph_dataset_2"), srn("graph_grandparent")),
        Some(srn("graph_grandparent")) -> Set(srn("graph_dataset_3"), srn("graph_dataset_4")),
        Some(srn("graph_dataset_1")) -> Set(),
        Some(srn("graph_dataset_2")) -> Set(),
        Some(srn("graph_dataset_3")) -> Set(),
        Some(srn("graph_dataset_4")) -> Set(),
      )
    )
  }

  test("Can produce a graph from an ad-hoc query") {
    def rn(s: String) = ResourceName(s)
    def srn(s: String) = ScopedResourceName(0, rn(s))

    val Right(foundTables) = tables.findTables(0, rn("graph_root"), "select * join @graph_dataset_4 on true", Map.empty)
    val graph = foundTables.queryGraph

    graph.root must equal (None)
    graph.graph must equal (
      Map(
        None -> Set(srn("graph_root"), srn("graph_dataset_4")),
        Some(srn("graph_root")) -> Set(srn("graph_parent_1"), srn("graph_parent_2")),
        Some(srn("graph_parent_1")) -> Set(srn("graph_dataset_1")),
        Some(srn("graph_parent_2")) -> Set(srn("graph_dataset_2"), srn("graph_grandparent")),
        Some(srn("graph_grandparent")) -> Set(srn("graph_dataset_3"), srn("graph_dataset_4")),
        Some(srn("graph_dataset_1")) -> Set(),
        Some(srn("graph_dataset_2")) -> Set(),
        Some(srn("graph_dataset_3")) -> Set(),
        Some(srn("graph_dataset_4")) -> Set(),
      )
    )
  }

  test("can rountdtrip a FoundTables through JSON") {
    def go(ft: tables.Result[FoundTables[MT]]): Unit = {
      val orig = ft.toOption.get
      val jsonized = JsonEncode.toJValue(orig)

      // first, can bounce it through UnparsedFoundTables and get the same JSON back out
      val unparsed = JsonDecode.fromJValue[UnparsedFoundTables[MT]](jsonized) match {
        case Right(ok) => ok
        case Left(err) => fail(err.english)
      }
      JsonEncode.toJValue(unparsed) must equal (jsonized)

      // Then, we can take that json and get the original FoundTables back out
      val parsed = JsonDecode.fromJValue[FoundTables[MT]](jsonized) match {
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
