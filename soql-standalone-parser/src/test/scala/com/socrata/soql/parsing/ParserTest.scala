package com.socrata.soql.parsing

import scala.util.parsing.input.NoPosition

import org.scalatest._
import org.scalatest.MustMatchers

import com.socrata.soql.ast._
import com.socrata.soql.parsing.standalone_exceptions.BadParse
import com.socrata.soql.environment.{FunctionName, ColumnName}

class ParserTest extends WordSpec with MustMatchers {
  def parseExpression(soql: String) = new StandaloneParser().expression(soql)

  def parseFull(soql: String) = new StandaloneParser().unchainedSelectStatement(soql)

  def expectFailure(expectedMsg: String, soql: String) =
    try {
      new StandaloneParser().expression(soql)
      fail("Unexpected success")
    } catch {
      case e: BadParse => e.message must equal (expectedMsg)
    }

  def ident(name: String) = ColumnOrAliasRef(None, ColumnName(name))(NoPosition)
  def functionCall(name: FunctionName, args: Seq[Expression], filter: Option[Expression], window: Option[WindowFunctionInfo]) = FunctionCall(name, args, filter, window)(NoPosition, NoPosition)
  def stringLiteral(s: String) = StringLiteral(s)(NoPosition)
  def numberLiteral(num: BigDecimal) = NumberLiteral(num)(NoPosition)

  "Parsing" should {
    "require a full `between' clause" in {
      expectFailure("Expected an expression, but got end of input", "x between")
      expectFailure("Expected `AND', but got end of input", "x between a")
      expectFailure("Expected an expression, but got end of input", "x between a and")
      expectFailure("Expected one of `BETWEEN', `IN', or `LIKE', but got end of input", "x not")
      expectFailure("Expected an expression, but got end of input", "x not between")
      expectFailure("Expected `AND', but got end of input", "x not between a")
    }

    "require a full `is null' clause" in {
      expectFailure("Expected one of `NULL' or `NOT', but got end of input", "x is")
      expectFailure("Expected `NULL', but got end of input", "x is not")
      expectFailure("Expected `NULL', but got `5'", "x is not 5")
      expectFailure("Expected one of `NULL' or `NOT', but got `5'", "x is 5")
    }

    "require an expression after `not'" in {
      expectFailure("Expected an expression, but got end of input", "not")
    }

    "reject a more-than-complete expression" in {
      expectFailure("Expected end of input, but got `y'", "x y")
    }

    "reject a null expression" in {
      expectFailure("Expected an expression, but got end of input", "")
    }

    "accept a lone identifier" in {
      parseExpression("a") must equal (ident("a"))
    }

    "require something after a dereference-dot" in {
      expectFailure("Expected an identifier, but got end of input", "a.")
    }

    "reject a system id after a dereference-dot" in {
      expectFailure("Expected a non-system identifier, but got `:id'", "a.:id")
    }

    "accept expr.identifier" in {
      parseExpression("a.b") must equal (functionCall(SpecialFunctions.Subscript, Seq(ident("a"), stringLiteral("b")), None, None))
    }

    "reject expr.identifier." in {
      expectFailure("Expected an identifier, but got end of input", "a.b.")
    }

    "reject expr[" in {
      expectFailure("Expected an expression, but got end of input", "a[")
    }

    "reject expr[expr" in {
      expectFailure("Expected `]', but got end of input", "a[2 * b")
    }

    "accept expr[expr]" in {
      parseExpression("a[2 * b]") must equal (
        functionCall(
          SpecialFunctions.Subscript,
          Seq(
            ident("a"),
            functionCall(
              SpecialFunctions.Operator("*"),
              Seq(
                numberLiteral(2),
                ident("b")), None, None)), None, None))
    }

    "reject expr[expr]." in {
      expectFailure("Expected an identifier, but got end of input", "a[2 * b].")
    }

    "accept expr[expr].ident" in {
      parseExpression("a[2 * b].c") must equal (functionCall(SpecialFunctions.Subscript, Seq(
        functionCall(
          SpecialFunctions.Subscript,
          Seq(
            ident("a"),
            functionCall(SpecialFunctions.Operator("*"), Seq(
              numberLiteral(2),
              ident("b")), None, None)), None, None),
        stringLiteral("c")), None, None))
    }

    "accept expr[expr].ident[expr]" in {
      parseExpression("a[2 * b].c[3]") must equal (functionCall(SpecialFunctions.Subscript, Seq(
        functionCall(
          SpecialFunctions.Subscript,
          Seq(
            functionCall(
              SpecialFunctions.Subscript,
              Seq(
                ident("a"),
                functionCall(SpecialFunctions.Operator("*"), Seq(
                  numberLiteral(2),
                  ident("b")), None, None)), None, None),
            stringLiteral("c")), None, None),
        numberLiteral(3)), None, None))
    }

    "accept modulo" in {
      parseExpression("11 % 2") must equal (
        functionCall(SpecialFunctions.Operator("%"), Seq(numberLiteral(11), numberLiteral(2)), None, None))
    }

    "^ has higher precedence than *" in {
      val expr = parseExpression("10 * 3 ^ 2")
      expr must equal (
        functionCall(SpecialFunctions.Operator("*"), Seq(
          numberLiteral(10),
          functionCall(SpecialFunctions.Operator("^"), Seq(numberLiteral(3), numberLiteral(2)), None, None)
        ), None, None)
      )
    }

    "allow offset/limit" in {
      val x = parseFull("select * offset 6 limit 5")
      x.offset must be (Some(BigInt(6)))
      x.limit must be (Some(BigInt(5)))
    }

    "allow limit/offset" in {
      val x = parseFull("select * limit 6 offset 5")
      x.limit must be (Some(BigInt(6)))
      x.offset must be (Some(BigInt(5)))
    }

    "allow only limit" in {
      val x = parseFull("select * limit 32")
      x.limit must be (Some(BigInt(32)))
      x.offset must be (None)
    }

    "allow only offset" in {
      val x = parseFull("select * offset 7")
      x.limit must be (None)
      x.offset must be (Some(BigInt(7)))
    }

    "allow search" in {
      val x = parseFull("select * search 'weather'")
      x.search.get must be ("weather")
    }

    "allow search before order by" in {
      val x = parseFull("select * search 'weather' order by x")
      x.search.get must be ("weather")
      x.orderBys must be (List(OrderBy(ident("x"), true, true)))
    }

    "allow order by before search" in {
      val x = parseFull("select * order by x search 'weather'")
      x.search.get must be ("weather")
      x.orderBys must be (Seq(OrderBy(ident("x"), true, true)))
    }

    "disallow order by before AND after search" in {
      a [BadParse] must be thrownBy { parseFull("select * order by x search 'weather' order by x") }
    }

    "disallow search before AND after order by" in {
      a [BadParse] must be thrownBy { parseFull("select * search 'weather' order by x search 'weather'") }
    }

    "not round trip" in {
      val x = parseFull("select * where not true")
      x.where.get.toString must be ("NOT TRUE")
    }

    "like round trip" in {
      val x = parseFull("select * where `a` like 'b'")
      x.where.get.toString must be ("`a` LIKE 'b'")
    }

    "not like round trip" in {
      val x = parseFull("select * where `a` not like 'b'")
      x.where.get.toString must be ("`a` NOT LIKE 'b'")
    }

    "search round trip" in {
      val x = parseFull("select * search 'weather'")
      val y = parseFull(x.toString)
      y must be (x)
    }

    "allow hints" in {
      val soql = "select hint(materialized) * limit 32"
      val x = parseFull(soql)
      x.hints must be (Vector(Materialized(SoQLPosition(1, 13, soql, 12))))
    }

    "count(disinct column)" in {
      val x = parseFull("select count(distinct column)")
      x.selection.expressions.head.expression.asInstanceOf[FunctionCall].functionName.name must be ("count_distinct")
      x.selection.expressions.head.expression.toString must be ("count(DISTINCT `column`)")
    }

    "count(column)" in {
      val x = parseFull("select count(column)")
      x.selection.expressions.head.expression.asInstanceOf[FunctionCall].functionName.name must be ("count")
      x.selection.expressions.head.expression.toString must be ("count(`column`)")
    }

    "count(*)" in {
      val x = parseFull("select count(*)")
      x.selection.expressions.head.expression.asInstanceOf[FunctionCall].functionName.name must be ("count/*")
      x.selection.expressions.head.expression.toString must be ("count(*)")
    }

    "count(*) window" in {
      val x = parseFull("select count(*) over (partition by column)")
      x.toString must be("SELECT count(*) OVER (PARTITION BY `column`)")
    }

    "count(column) window" in {
      val x = parseFull("select count(column_a) over (partition by column_b)")
      x.toString must be("SELECT count(`column_a`) OVER (PARTITION BY `column_b`)")
    }

    "window function over partition order round trip" in {
      val x = parseFull("select row_number() over(partition by x, y order by m, n)")
      x.selection.expressions.head.expression.toCompactString must be ("row_number() OVER (PARTITION BY `x`, `y` ORDER BY `m` ASC NULL LAST, `n` ASC NULL LAST)")
    }

    "window function over partition order desc round trip" in {
      val x = parseFull("select row_number() over(partition by x, y order by m desc null last, n)")
      x.selection.expressions.head.expression.toCompactString must be ("row_number() OVER (PARTITION BY `x`, `y` ORDER BY `m` DESC NULL LAST, `n` ASC NULL LAST)")
    }

    "window function over partition round trip" in {
      val x = parseFull("select avg(x) over(partition by x, y)")
      x.selection.expressions.head.expression.toCompactString must be ("avg(`x`) OVER (PARTITION BY `x`, `y`)")
    }

    "window function over order round trip" in {
      val x = parseFull("select avg(x) over(order by m, n)")
      x.selection.expressions.head.expression.toString must be ("avg(`x`) OVER (ORDER BY `m` ASC NULL LAST, `n` ASC NULL LAST)")
    }

    "window function empty over round trip" in {
      val x = parseFull("select avg(x) over()")
      x.selection.expressions.head.expression.toString must be ("avg(`x`) OVER ()")
    }

    "window function over partition frame" in {
      val x = parseFull("select avg(x) over(order by m range 123 PRECEDING)")
      x.selection.expressions.head.expression.toString must be ("avg(`x`) OVER (ORDER BY `m` ASC NULL LAST RANGE 123 PRECEDING)")
    }

    "window frame clause should start with rows or range, not row" in {
      expectFailure("Expected one of `)', `RANGE', `ROWS', `,', `NULL', `NULLS', `ASC', or `DESC', but got `row'", "avg(x) over(order by m row 123 PRECEDING)")
    }

    "reject pipe query where right side is not a leaf." in {
      val parser = new StandaloneParser()
      try {
        parser.binaryTreeSelect("SELECT 1 |> (SELECT 2 UNION SELECT 3 FROM @t2 )")
        fail("Unexpected success")
      } catch {
        case e: BadParse.ExpectedLeafQuery =>
          // ok good
      }
    }

    "lateral join" in {
      val x = parseFull("select c1 join (select c11 from @t1) as j1 on true join lateral (select c21 from @t2 where c21=@j1.c11) as j2 on true")
      x.joins(0).lateral must be (false)
      x.joins(1).lateral must be (true)
    }

    "select empty" in {
      val x = parseFull("select")
      val s = x.selection
      s.expressions.isEmpty must be (true)
      s.allSystemExcept.isEmpty must be (true)
      s.allUserExcept.isEmpty must be (true)
      x.toString must be ("SELECT")
    }

    // def show[T](x: => T) {
    //   try {
    //     println(x)
    //   } catch {
    //     case l: LexerError =>
    //       val p = l.position
    //       println("[" + p.line + "." + p.column + "] failure: "+ l.getClass.getSimpleName)
    //       println()
    //       println(p.longString)
    //   }
    // }

    //   show(new LexerReader("").toStream.force)
    //   show(new LexerReader("- -1*a(1,b)*(3)==2 or x between 1 and 3").toStream.force)
    //   show(p.expression("- -1*a(1,b)*(3)==2 or x not between 1 and 3"))
    //   show(p.expression("x between a is null and a between 5 and 6 or f(x x"))
    //   show(p.expression("x x"))

    //   LEXER TESTS
    //   show(p.expression("étäøîn"))
    //   show(p.expression("_abc + -- hello world!\n123 -- gnu"))
    //   show(p.expression("\"gnu\""))
    //   show(p.expression("\"\\U01D000\""))
    //   show(p.selectStatement("SELECT x AS `where`, y AS `hither-thither`")) // this one's only a semi-lexer-test
    //   show(p.limit("12e1"))
    //   show(p.expression("`hello world`"))
    //   show(p.identifier("__gnu__"))

    //   show(p.expression("* 8"))
    //   show(p.orderings("a,b desc,c + d/e"))
    //   show(p.orderings(""))
    //   show(p.orderings("a,"))
    //   show(p.orderings("a ascdesc"))
    //   show(p.selection(""))
    //   show(p.selection("a,b,c,"))
    //   show(p.selection("a,b as,c"))
    //   show(p.selection("a,b as x,c"))
    //   show(p.selection("a,b as x,c as y"))
    //   show(p.selection("a,b as x,c as"))
    //   show(p.selectStatement(""))
    //   show(p.selectStatement("x"))
    //   show(p.selectStatement("select"))
    //   show(p.selectStatement("select a, b where"))
    //   show(p.selectStatement("select a, b group"))
    //   show(p.selectStatement("select a, b order"))
    //   show(p.selectStatement("select a, b having"))
    //   show(p.selectStatement("select * where x group by y having z order by w"))
    //   show(p.selectStatement("select *"))
    //   show(p.selectStatement("select *("))
    //   show(p.selectStatement("select *(except"))
    //   show(p.selectStatement("select *(except a"))
    //   show(p.selectStatement("select *(except a)"))
    //   show(p.selectStatement("select *(except a,"))
    //   show(p.selectStatement("select *(except a,b)"))
    //   show(p.orderings("x, y asc null last"))
    //   show(p.selectStatement("select * order by x, y gnu,"))
    //   show(p.expression("-a :: b :: c"))
    //   show(p.selectStatement("select * order by x, y limit 5"))
    //   show(p.expression("a(1) || 'blah'"))
    //   show(p.selectStatement("select x"))

    //   show(p.expression("count(__gnu__)+a+b").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    //   show(p.expression("count(a) == 'hello, world!  This is a smiling gnu.'").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    //   show(p.expression("count(a) == `hello-world`").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    //   show(p.expression("`-world` + 2").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    //   show(p.expression("world is not null").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    //   show(p.expression("1 + `-world` = `a-` - 1").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    //   show(p.expression(":id - 1").asInstanceOf[Parsers#Success[Expression]].result.toSyntheticIdentifierBase)
    //   show(p.expression("count(a) == 'hello, world!  This is a smiling gnu.'"))
    //   show(p.selectStatement("select x as :x"))

    //   show(p.selectStatement("select :*"))
    //   show(p.selectStatement("select *"))
    //   show(p.selectStatement("select :*,"))
    //   show(p.selectStatement("select :*,a"))
    //   show(p.selectStatement("select *,"))
    //   show(p.selectStatement("select *,a"))
    //   show(p.selectStatement("select :*,*"))
    //   show(p.selectStatement("select :*,*,"))
    //   show(p.selectStatement("select :*,*,a"))
    //   show(p.selectStatement("select :*(except a)"))
    //   show(p.selectStatement("select :*(except :a)"))
    //   show(p.selectStatement("select *(except :a)"))
    //   show(p.selectStatement("select *(except a)"))
  }
}
