package com.socrata.soql.parsing

import org.scalatest.{FunSpec, MustMatchers}

class BinaryTreeSelectTest extends FunSpec with MustMatchers {
  val parser = new StandaloneParser()

  describe("Previous") {
    it("of pipe is the right query") {
      val soql = "SELECT 1 as a |> SELECT 2 as b"
      val selects = parser.binaryTreeSelect(soql)
      val columnName = selects.outputSchemaLeaf.selection.expressions.head.name.get._1.name
      columnName must be("b")
    }

    it("of union is the left query") {
      val soql = "SELECT 1 as a UNION SELECT 2 as b"
      val selects = parser.binaryTreeSelect(soql)
      val columnName = selects.outputSchemaLeaf.selection.expressions.head.name.get._1.name
      columnName must be("a")
    }
  }

  describe("Seq") {
    it("of pipe is the right query") {
      val soql = "SELECT 1 as a |> SELECT 2 as b |> SELECT 3 as c"
      val selects = parser.binaryTreeSelect(soql)
      selects.seq.map(_.toString) must be (Seq("SELECT 1 AS a", "SELECT 2 AS b", "SELECT 3 AS c"))
    }
  }
}
