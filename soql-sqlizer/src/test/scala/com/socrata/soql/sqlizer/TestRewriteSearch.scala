package com.socrata.soql.sqlizer

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer._
import com.socrata.soql.functions.MonomorphicFunction

object TestRewriteSearch extends RewriteSearch[TestHelper.TestMT] {
  type TestMT = TestHelper.TestMT
  import TestTypeInfo.hasType

  override def searchTerm(schema: Iterable[(ColumnLabel, Rep[TestHelper.TestMT])]): Option[Doc[Nothing]] = {
    val term =
      schema.toSeq.flatMap { case (label, rep) =>
        rep.typ match {
          case TestText =>
            rep.expandedDatabaseColumns(label)
          case _ =>
            Nil
        }
      }.map { col =>
        Seq(col, d"''").funcall(d"coalesce")
      }

    if(term.nonEmpty) {
      Some(term.concatWith { (a: Doc[Nothing], b: Doc[Nothing]) => a +#+ d"|| ' ' ||" +#+ b })
    } else {
      None
    }
  }

  override val searchBeforeQuery = true

  override def concat = TestFunctions.Concat.monomorphic.get

  override def denull(string: Expr) =
    FunctionCall[TestMT](coalesce, Seq(string, litText("")))(FuncallPositionInfo.Synthetic)

  override def fieldsOf(expr: Expr): Seq[Expr] =
    expr.typ match {
      case TestText => Seq(expr)
      case _ => Nil
    }

  override def litBool(b: Boolean): Expr = LiteralValue[TestMT](TestBoolean(b))(AtomicPositionInfo.Synthetic)
  override def litText(s: String): Expr = LiteralValue[TestMT](TestText(s))(AtomicPositionInfo.Synthetic)

  override def isText(t: TestType) = t == TestText
  override def isBoolean(t: TestType) = t == TestBoolean

  override def mkAnd(left: Expr,right: Expr): Expr =
    FunctionCall[TestMT](
      TestFunctions.And.monomorphic.get,
      Seq(left, right)
    )(FuncallPositionInfo.Synthetic)

  override def mkOr(left: Expr,right: Expr): Expr =
    FunctionCall[TestMT](
      TestFunctions.Or.monomorphic.get,
      Seq(left, right)
    )(FuncallPositionInfo.Synthetic)

  override def plainToTsQuery: MonomorphicFunction = ???
  override def toTsVector: MonomorphicFunction = ???
  override def tsSearch: MonomorphicFunction = ???

  private val coalesce = MonomorphicFunction(TestFunctions.Coalesce, Map("a" -> TestText))
}
