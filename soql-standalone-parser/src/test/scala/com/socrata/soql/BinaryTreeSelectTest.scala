package com.socrata.soql.parsing

import com.socrata.soql.{Compound, Leaf}
import org.scalatest.{FunSpec, MustMatchers}

class BinaryTreeSelectTest extends FunSpec with MustMatchers {
  val parser = new StandaloneParser()

  describe("Previous") {
    it("of pipe is the right query") {
      val soql = "SELECT 1 as a |> SELECT 2 as b"
      val selects = parser.binaryTreeSelect(soql)
      val columnName = selects.outputSchema.leaf.selection.expressions.head.name.get._1.name
      columnName must be("b")
    }

    it("of union is the left query") {
      val soql = "SELECT 1 as a UNION SELECT 2 as b"
      val selects = parser.binaryTreeSelect(soql)
      val columnName = selects.outputSchema.leaf.selection.expressions.head.name.get._1.name
      columnName must be("a")
    }
  }

  describe("Seq") {
    it("of pipe is the right query") {
      val soql = "SELECT 1 as a |> SELECT 2 as b |> SELECT 3 as c"
      val selects = parser.binaryTreeSelect(soql)
      selects.seq.map(_.toString) must be (Seq("SELECT 1 AS `a`", "SELECT 2 AS `b`", "SELECT 3 AS `c`"))
    }
  }

  describe("General") {
    it ("leftMost update compare uses reference equality") {
      val soql = "SELECT 1 as a UNION SELECT 1 as a"
      val binaryTree = parser.binaryTreeSelect(soql)
      val compound@Compound(_, l@Leaf(_, _), r@Leaf(_, _)) = binaryTree
      l.eq(r) must be (false)
      val leftCopy = l.copy()
      leftCopy.eq(l) must be (false)
      leftCopy.equals(l) must be (true)
      compound.leftMost.eq(l) must be(true)
      val Compound(_, nl, lr) = binaryTree.replace(l, leftCopy)
      nl.eq(leftCopy) must be (true) // leftmost is updated
      lr.eq(r) must be (true) // right is not changed
    }
  }

  describe("Parentheses are preserved") {
    it("in the right side") {
      val soql = "(SELECT 1 as a UNION SELECT 2 as b UNION (SELECT 3 as c))"
      val compound@Compound(_, _, _) = parser.binaryTreeSelect(soql)
      compound.toString must be("(SELECT 1 AS `a` UNION SELECT 2 AS `b` UNION (SELECT 3 AS `c`))")
      compound.inParen must be(true)
      compound.left.inParen must be(false)
      compound.right.asLeaf.isDefined must be(true)
      compound.right.inParen must be(true)
    }

    it("in the left side") {
      val soql = "(SELECT 1 as a UNION SELECT 2 as b) UNION SELECT 3 as c"
      val compound@Compound(_, _, _) = parser.binaryTreeSelect(soql)
      compound.toString must be("(SELECT 1 AS `a` UNION SELECT 2 AS `b`) UNION SELECT 3 AS `c`")
      compound.inParen must be(false)
      compound.left.inParen must be(true)
      compound.right.inParen must be(false)
    }

    it ("in leaf") {
      val soql = "(SELECT 1 as a)"
      val leaf@Leaf(_, _) = parser.binaryTreeSelect(soql)
      leaf.toString must be("(SELECT 1 AS `a`)")
      leaf.inParen must be(true)
    }

    it ("in wrapped") {
      val soql = "SELECT 1 as a"
      val leaf@Leaf(_, _) = parser.binaryTreeSelect(soql)
      leaf.toString must be("SELECT 1 AS `a`")
      leaf.inParen must be(false)
      val wrapped = leaf.wrapInParen()
      wrapped.toString must be("(SELECT 1 AS `a`)")
      wrapped.inParen must be(true)
    }
  }
}
