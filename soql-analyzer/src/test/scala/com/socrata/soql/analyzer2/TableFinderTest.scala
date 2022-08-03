package com.socrata.soql.analyzer2

import org.scalatest.FunSuite
import org.scalatest.MustMatchers

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast.Select
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.{StandaloneParser, AbstractParser}

class TableFinderTest extends FunSuite with MustMatchers {
  sealed abstract class Thing
  case class D(schema: Map[String, String]) extends Thing
  case class Q(scope: Int, parent: ResourceName, soql: String) extends Thing
  case class U(scope: Int, soql: String) extends Thing

  class MockTableFinder(raw: Map[(Int, String), Thing]) extends TableFinder {
    private val tables = raw.iterator.map { case ((scope, rawResourceName), thing) =>
      val converted = thing match {
        case D(rawSchema) =>
          Dataset(
            DatabaseTableName(rawResourceName),
            OrderedMap() ++ rawSchema.iterator.map {case (rawColumnName, ct) =>
              ColumnName(rawColumnName) -> ct
            }
          )
        case Q(scope, parent, soql) =>
          Query(scope, parent, soql, None)
        case U(scope, soql) =>
          TableFunction(scope, soql, OrderedMap.empty)
      }
      (scope, ResourceName(rawResourceName)) -> converted
    }.toMap

    type ResourceNameScope = Int
    type ParseError = LexerParserException
    type ColumnType = String

    protected def lookup(scope: Int, name: ResourceName): Either[LookupError, TableDescription] = {
      tables.get((scope, name)) match {
        case Some(schema) =>
          Right(schema)
        case None =>
          Left(LookupError.NotFound)
      }
    }

    protected def parse(soql: String, udfParamsAllowed: Boolean): Either[ParseError, BinaryTree[Select]] = {
      try {
        Right(
          new StandaloneParser(AbstractParser.defaultParameters.copy(allowHoles = udfParamsAllowed)).
            binaryTreeSelect(soql)
        )
      } catch {
        case e: ParseError =>
          Left(e)
      }
    }

    private def parsed(thing: TableDescription) = {
      thing match {
        case ds: Dataset => ds.toParsed
        case Query(scope, parent, soql, params) => ParsedTableDescription.Query(scope, parent, parse(soql, false).getOrElse(fail("broken soql fixture 1")), params)
        case TableFunction(scope, soql, params) => ParsedTableDescription.TableFunction(scope, parse(soql, false).getOrElse(fail("broken soql fixture 2")), params)
      }
    }

    def apply(names: (Int, String)*): Success[TableMap] = {
      val r = names.map { case (scope, n) =>
        val name = ResourceName(n)
        (scope, name) -> parsed(tables((scope, name)))
      }.toMap

      r.size must equal (names.length)

      Success(new TableMap(r))
    }

    def notFound(scope: Int, name: String) =
      Error.NotFound((scope, ResourceName(name)))
  }

  val tables = new MockTableFinder(
    Map(
      (0, "t1") -> D(Map(
        "key" -> "integer",
        "value" -> "thing"
      )),
      (0, "t2") -> D(Map(
        "key" -> "integer",
        "value" -> "otherthing"
      )),
      (0, "t3") -> Q(0, ResourceName("t2"), "select *"),
      (0, "t4") -> U(0, "select * from @t2"),
      (0, "t5") -> Q(1, ResourceName("t1"), "select *"),
      (0, "t6") -> Q(1, ResourceName("t2"), "select *"), // t2 exists in scope 0 but not in scope 1
      (1, "t1") -> D(Map())
    )
  )

  test("can find a table") {
    tables.findTables(0, "select * from @t1").map(_.tableMap) must equal (tables((0, "t1")))
  }

  test("can fail to find a table") {
    tables.findTables(0, "select * from @doesnt-exist").map(_.tableMap) must equal (tables.notFound(0, "doesnt-exist"))
  }

  test("can find a table implicitly") {
    tables.findTables(0, ResourceName("t1"), "select key, value").map(_.tableMap) must equal (tables((0, "t1")))
  }

  test("can find a joined table") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t2 on @t2.key = key").map(_.tableMap) must equal (tables((0, "t1"), (0, "t2")))
  }

  test("can find a query that accesses another table") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t3 on @t3.key = key").map(_.tableMap) must equal (tables((0, "t1"), (0, "t2"), (0, "t3")))
  }

  test("can find a query that accesses another table via udf") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t4(1,2,3) on @t4.key = key").map(_.tableMap) must equal (tables((0, "t1"), (0, "t2"), (0, "t4")))
  }

  test("can find a in a different scope") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t5 on @t5.key = key").map(_.tableMap) must equal (tables((0, "t1"), (0, "t5"), (1, "t1")))
  }

  test("can fail to find a query in a different scope") {
    tables.findTables(0, ResourceName("t1"), "select key, value join @t6 on @t6.key = key").map(_.tableMap) must equal (tables.notFound(1, "t2"))
  }
}
