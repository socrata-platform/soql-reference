package com.socrata.soql.parsing

import org.scalatest._
import org.scalatest.MustMatchers

import com.socrata.soql.ast._
import com.socrata.soql.environment.{HoleName, TypeName, TableName}

class RewriteTest extends FunSuite with MustMatchers {
  def defineUDF(soql: String, params: (String, TypeName)*) = {
    val parser =
      new StandaloneParser(AbstractParser.defaultParameters.copy(
                             allowHoles = true,
                             allowJoinFunctions = true
                           ))
    val body = parser.binaryTreeSelect(soql)
    UDF(arguments = params.map {
          case (n, t) => HoleName(n) -> t
        },
        body = body)
  }

  val UDFs = Map(
    TableName("_foo") -> defineUDF("SELECT 5 from @haha-haha WHERE ?x = 3",
                                   "x" -> TypeName("number")),
    TableName("_bar") -> defineUDF("SELECT 1 from @some-thng join @foo(?x) as foo on true WHERE name = ?name",
                                   "name" -> TypeName("string"),
                                   "x" -> TypeName("number")),
    TableName("_baz") -> defineUDF("SELECT 1 from @single_row")
  )

  def parseSelect(soql: String) = {
    val parser =
      new StandaloneParser(AbstractParser.defaultParameters.copy(
                             allowJoinFunctions = true
                           ))
    try {
      parser.binaryTreeSelect(soql)
    } catch {
      case e: Exception =>
        System.err.println(e)
        throw e
    }
  }

  test("Can find non-transitively referenced join functions") {
    val selection =
      parseSelect("""
        select * join @foo(5, 6) as f on true join (select * from @bleh-bleh join @baz() as b on true) as blehbleh on false
      """)
    Select.findDirectlyReferencedJoinFuncs(selection) must equal (Set(TableName("_foo"), TableName("_baz")))
  }

  test("Can expand transitively referenced join functions") {
    val selection =
      parseSelect("""
        select * join @bar("hello", 6) as expr_3 on true -- the alias is to check the avoidance of existing aliases
      """)

    Select.rewriteJoinFuncs(selection, UDFs) must equal (parseSelect("""
      SELECT
        *
      JOIN (
        SELECT
          @expr_1.*
        FROM @single_row
          JOIN (
            SELECT
              'hello' :: string AS name,
              6 :: number AS x
            FROM @single_row
          ) AS vars_2 ON TRUE
          JOIN LATERAL (
            SELECT
              1
            FROM @some-thng
              JOIN (
                SELECT
                  @expr_4.*
                FROM @single_row
                  JOIN (
                    SELECT
                      @vars_2.`x` :: number AS x
                    FROM @single_row
                  ) AS vars_5 ON TRUE
                  JOIN LATERAL (
                    SELECT
                      5
                    FROM @haha-haha
                    WHERE
                      @vars_5.`x` = 3
                 ) AS expr_4 ON TRUE
              ) AS foo ON TRUE
            WHERE
              `name` = @vars_2.`name`
          ) AS expr_1 ON TRUE
        ) AS expr_3 ON TRUE
    """))
  }

  test("Will fail to expand on parameter length mismatch") {
    val selection =
      parseSelect("""
        select * join @foo(5, 6) as f on true
      """)
    assertThrows[MismatchedParameterCount] {
      Select.rewriteJoinFuncs(selection, UDFs)
    }
  }


  test("Will fail to expand on unknown function") {
    val selection =
      parseSelect("""
        select * join @whatever(5, 6) as w on true
      """)
    assertThrows[UnknownUDF] {
      Select.rewriteJoinFuncs(selection, UDFs)
    }
  }
}
