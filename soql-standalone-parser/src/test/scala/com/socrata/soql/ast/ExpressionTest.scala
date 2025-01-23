package com.socrata.soql.ast

import scala.util.parsing.input.NoPosition

import org.scalatest._

import com.socrata.soql.environment.{FunctionName, ColumnName}
import com.socrata.soql.parsing.{AbstractParser, StandaloneParser}

class ExpressionTest extends FunSuite with MustMatchers {
  private val parser = new StandaloneParser(AbstractParser.defaultParameters.copy(allowInSubselect = true))

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
        parser.binaryTreeSelect("select 1")
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
        parser.binaryTreeSelect("select 1")
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
        parser.binaryTreeSelect("select 1")
      )(NoPosition, NoPosition)

    expr.doc.toString must equal ("f(null) IN (SELECT 1)")
  }

  test("Docifying surrounds InSubselect with parens if necessary") {
    val expr =
      FunctionCall(
        SpecialFunctions.Operator("="),
        Seq(
          BooleanLiteral(true)(NoPosition),
          InSubselect(
            ColumnOrAliasRef(None, ColumnName("x"))(NoPosition),
            false,
            parser.binaryTreeSelect("select 1")
          )(NoPosition, NoPosition)
        )
      )(NoPosition, NoPosition)

    expr.doc.toString must equal ("TRUE = (`x` IN (SELECT 1))")
  }

  test("Docifying does not surround InSubselect with parens if not necessary") {
    val expr =
      FunctionCall(
        SpecialFunctions.Operator("or"),
        Seq(
          BooleanLiteral(true)(NoPosition),
          InSubselect(
            ColumnOrAliasRef(None, ColumnName("x"))(NoPosition),
            false,
            parser.binaryTreeSelect("select 1")
          )(NoPosition, NoPosition)
        )
      )(NoPosition, NoPosition)

    expr.doc.toString must equal ("TRUE or `x` IN (SELECT 1)")
  }

  test("Parsing IN (subselect) as higher precedence") {
    val parsed = parser.expression("true or x in (select 1)")
    parsed must equal (
      FunctionCall(
        SpecialFunctions.Operator("or"),
        Seq(
          BooleanLiteral(true)(NoPosition),
          InSubselect(
            ColumnOrAliasRef(None, ColumnName("x"))(NoPosition),
            false,
            parser.binaryTreeSelect("select 1")
          )(NoPosition, NoPosition)
        )
      )(NoPosition, NoPosition)
    )
  }

  test("Parsing IN (subselect) as lower precedence") {
    val parsed = parser.expression("true = x in (select 1)")
    parsed must equal (
      InSubselect(
        FunctionCall(
          SpecialFunctions.Operator("="),
          Seq(
            BooleanLiteral(true)(NoPosition),
            ColumnOrAliasRef(None, ColumnName("x"))(NoPosition)
          )
        )(NoPosition, NoPosition),
        false,
        parser.binaryTreeSelect("select 1")
      )(NoPosition, NoPosition)
    )
  }
}
