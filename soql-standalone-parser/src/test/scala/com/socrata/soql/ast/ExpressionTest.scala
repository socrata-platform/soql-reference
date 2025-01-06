package com.socrata.soql.ast

import scala.util.parsing.input.NoPosition

import org.scalatest._

import com.socrata.soql.environment.FunctionName
import com.socrata.soql.parsing.StandaloneParser

class ExpressionTest extends FunSuite with MustMatchers {
  test("Docifying InSubselect inserts parentheses if the scrutinee is a low-precedence operator") {
    val expr =
      InSubselect(
        FunctionCall(
          SpecialFunctions.Operator("or"),
          Seq(
            NullLiteral()(NoPosition),
            NullLiteral()(NoPosition)
          )
        )(NoPosition, NoPosition),
        false,
        new StandaloneParser().binaryTreeSelect("select 1")
      )(NoPosition, NoPosition)

    expr.doc.toString must equal ("(null or null) IN (SELECT 1)")
  }

  test("Docifying InSubselect does not insert parentheses if the scrutinee is a high-precedence operator") {
    val expr =
      InSubselect(
        FunctionCall(
          SpecialFunctions.Operator("+"),
          Seq(
            NullLiteral()(NoPosition),
            NullLiteral()(NoPosition)
          )
        )(NoPosition, NoPosition),
        false,
        new StandaloneParser().binaryTreeSelect("select 1")
      )(NoPosition, NoPosition)

    expr.doc.toString must equal ("null + null IN (SELECT 1)")
  }


  test("Docifying InSubselect does not insert parentheses if the scrutinee is an ordinary function call") {
    val expr =
      InSubselect(
        FunctionCall(
          FunctionName("f"),
          Seq(
            NullLiteral()(NoPosition)
          )
        )(NoPosition, NoPosition),
        false,
        new StandaloneParser().binaryTreeSelect("select 1")
      )(NoPosition, NoPosition)

    expr.doc.toString must equal ("f(null) IN (SELECT 1)")
  }
}
