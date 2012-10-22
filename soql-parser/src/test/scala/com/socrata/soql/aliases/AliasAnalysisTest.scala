package com.socrata.soql.aliases

import scala.util.parsing.input.{Position, NoPosition}

import org.scalatest._
import org.scalatest.matchers.MustMatchers

import com.socrata.soql.parsing.{LexerReader, Parser}

import com.socrata.soql.{SchemalessDatasetContext, UntypedDatasetContext}
import com.socrata.soql.names.ColumnName
import com.socrata.soql.ast._
import com.socrata.collection.{OrderedMap, OrderedSet}

class AliasAnalysisTest extends WordSpec with MustMatchers {
  def columnName(name: String)(implicit ctx: SchemalessDatasetContext) =
    ColumnName(name)

  def columnNames(names: String*)(implicit ctx: SchemalessDatasetContext) =
    OrderedSet(names.map(columnName): _*)

  def fixtureContext(cols: String*) =
    new UntypedDatasetContext {
      private implicit def dsc = this
      val locale = com.ibm.icu.util.ULocale.US
      lazy val columns = columnNames(cols: _*)
    }

  def fixturePosition(l: Int, c: Int): Position = new Position {
    def line = l
    def column = c
    def lineContents = " " * c
  }

  def se(e: String)(implicit ctx: SchemalessDatasetContext): SelectedExpression =
    SelectedExpression(expr(e), None)

  def se(e: String, name: String, position: Position)(implicit ctx: SchemalessDatasetContext): SelectedExpression =
    SelectedExpression(expr(e), Some((ColumnName(name), position)))

  implicit def selections(e: String)(implicit ctx: SchemalessDatasetContext): Selection = {
    val parser = new Parser
    parser.selection(e)
  }
  def selectionsNoPos(e: String)(implicit ctx: SchemalessDatasetContext): Selection = selections(e)
  def expr(e: String)(implicit ctx: SchemalessDatasetContext): Expression = {
    val parser = new Parser
    parser.expression(e)
  }
  def ident(e: String)(implicit ctx: SchemalessDatasetContext): ColumnName = {
    val parser = new Parser
    parser.identifier(new LexerReader(e)) match {
      case parser.Success(parsed, _) => ColumnName(parsed._1)
      case failure => fail("Unable to parse expression fixture " + e + ": " + failure)
    }
  }
  def unaliased(names: String*)(pos: Position)(implicit ctx: SchemalessDatasetContext) = names.map { i => SelectedExpression(ColumnOrAliasRef(columnName(i)), None) }

  "processing a star" should {
    implicit val ctx = fixtureContext()
    val pos = fixturePosition(4, 3)

    "expand to all input columns when there are no exceptions" in {
      // TODO: check the positions
      AliasAnalysis.processStar(StarSelection(Seq.empty).positionedAt(pos), columnNames("a","b","c")) must equal (unaliased("a", "b", "c")(pos))
    }

    "expand to the empty list when there are no columns" in {
      AliasAnalysis.processStar(StarSelection(Seq.empty).positionedAt(pos), columnNames()) must equal (Seq.empty)
    }

    "expand with exclusions exluded" in {
      // TODO: check the positions
      AliasAnalysis.processStar(StarSelection(Seq((ident("b"), fixturePosition(5, 3)))).positionedAt(pos), columnNames("a","b","c")) must equal (unaliased("a","c")(pos))
    }

    "throw an exception if an exception does not occur in the column-set" in {
      // TODO: Check the position
      evaluating { AliasAnalysis.processStar(StarSelection(Seq((ident("not_there"), NoPosition))).positionedAt(pos), columnNames("a","c")) } must produce[NoSuchColumnException]
    }

    "throw an exception if an exception occurs more than once" in {
        // TODO: Check the position
      evaluating { AliasAnalysis.processStar(StarSelection(Seq((ident("a"), NoPosition), (ident("a"), NoPosition))).positionedAt(pos), columnNames("a","c")) } must produce[RepeatedExceptionException]
    }
  }

  "expanding a selection" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")
    val pos = fixturePosition(5, 12)
    val pos2 = fixturePosition(32, 1)
    val someSelections = selections("2+2,hello,avg(gnu) as average").expressions

    "return the input if there were no stars" in {
      AliasAnalysis.expandSelection(Selection(None, None, someSelections)) must equal (someSelections)
    }

    "return the system columns if there was a :*" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(Some(StarSelection(Seq.empty).positionedAt(pos)), None, someSelections)) must equal (unaliased(":a",":b")(pos) ++ someSelections)
    }

    "return the un-excepted columns if there was a :*" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(Some(StarSelection(Seq((ident(":a"), NoPosition))).positionedAt(pos)), None, someSelections)) must equal (unaliased(":b")(pos) ++ someSelections)
    }

    "return the user columns if there was a *" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(None, Some(StarSelection(Seq.empty).positionedAt(pos)), someSelections)) must equal (unaliased("c","d","e")(pos) ++ someSelections)
    }

    "return the un-excepted user columns if there was a *" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(None, Some(StarSelection(Seq((ident("d"), NoPosition),(ident("e"), NoPosition))).positionedAt(pos)), someSelections)) must equal (unaliased("c")(pos) ++ someSelections)
    }

    "return the all user columns if there was a :* and a *" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(Some(StarSelection(Seq.empty).positionedAt(pos)), Some(StarSelection(Seq.empty).positionedAt(pos2)), someSelections)) must equal (unaliased(":a",":b")(pos) ++ unaliased("c","d","e")(pos2) ++ someSelections)
    }
  }

  "assigning (semi-)explicit aliases" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")

    "assign aliases to simple columns" in {
      val ss = selections("2+2,hello,world,avg(gnu),(x)").expressions
      AliasAnalysis.assignExplicitAndSemiExplicit(ss) must equal (
        Seq(ss(0), se("hello","hello", ss(1).expression.position), se("world", "world", ss(2).expression.position), ss(3), ss(4))
      )
    }

    "accept aliases" in {
      val ss = selections("2+2 as four,hello as x,avg(gnu),world,(x)").expressions
      AliasAnalysis.assignExplicitAndSemiExplicit(ss) must equal (
        Seq(ss(0), ss(1), ss(2), se("world","world",ss(3).expression.position), ss(4))
      )
    }

    "reject duplicate aliases when one is explicit and the other semi-explicit" in {
      val ss = selections("2+2 as four, four").expressions
      evaluating { AliasAnalysis.assignExplicitAndSemiExplicit(ss) } must produce[DuplicateAliasException]
    }

    "reject duplicate aliases when both are semi-explicit" in {
      val ss = selections("four, four").expressions
      evaluating { AliasAnalysis.assignExplicitAndSemiExplicit(ss) } must produce[DuplicateAliasException]
    }

    "reject duplicate aliases when both are explicit" in {
      val ss = selections("2 + 2 as four, 1 + 3 as four").expressions
      evaluating { AliasAnalysis.assignExplicitAndSemiExplicit(ss) } must produce[DuplicateAliasException]
    }
  }

  "creating an implicit alias" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")

    "select a name" in {
      AliasAnalysis.implicitAlias(expr("a"), Set()) must equal (columnName("a"))
      AliasAnalysis.implicitAlias(expr("a + b"), Set()) must equal (columnName("a_b"))
    }

    "select a name not in use" in {
      AliasAnalysis.implicitAlias(expr("a + b"), columnNames("a_b")) must equal (columnName("a_b_1"))
      AliasAnalysis.implicitAlias(expr("c"), Set()) must equal (columnName("c_1")) // checking against the dataset context
      AliasAnalysis.implicitAlias(expr("a + b"), columnNames("a_b", "a_b_1", "a_b_2")) must equal (columnName("a_b_3"))
    }

    "not add unnecessary underscores when suffixing a disambiguator" in {
      AliasAnalysis.implicitAlias(expr("a + b_"), Set(columnName("a_b_"))) must equal (columnName("a_b_1"))
      AliasAnalysis.implicitAlias(expr("a + `b-`"), Set(columnName("a_b-"))) must equal (columnName("a_b-1"))
    }
  }

  "assigning implicits" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")

    "assign non-conflicting aliases" in {
      AliasAnalysis.assignImplicit(Seq(se(":a + 1", "c", NoPosition), se(":a + 1"), se("-c"), se("-d"), se("e", "d_1", NoPosition))) must equal (
        Map(
          columnName("c") -> expr(":a + 1"),
          columnName("a_1") -> expr(":a + 1"),
          columnName("c_1") -> expr("-c"),
          columnName("d_2") -> expr("-d"),
          columnName("d_1") -> expr("e")
        )
      )
    }

    "preserve ordering" in {
      AliasAnalysis.assignImplicit(Seq(se(":a + 1", "c", NoPosition), se(":a + 1"), se("-c"), se("-d"), se("e", "d_1", NoPosition))).toSeq must equal (
        Seq(
          columnName("c") -> expr(":a + 1"),
          columnName("a_1") -> expr(":a + 1"),
          columnName("c_1") -> expr("-c"),
          columnName("d_2") -> expr("-d"),
          columnName("d_1") -> expr("e")
        )
      )
    }

    "reject assigning to a straight identifier" in {
      evaluating { AliasAnalysis.assignImplicit(Seq(se("q"))) } must produce[AssertionError]
    }
  }

  "the whole flow" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")

    "expand *" in {
      AliasAnalysis(selections("*")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          columnName("c") -> expr("c"),
          columnName("d") -> expr("d"),
          columnName("e") -> expr("e")
        ),
        Seq("c","d","e").map(columnName)
      ))
    }

    "expand :*" in {
      AliasAnalysis(selections(":*")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          columnName(":a") -> expr(":a"),
          columnName(":b") -> expr(":b")
        ),
        Seq(":a", ":b").map(columnName)
      ))
    }

    "expand :*,*" in {
      AliasAnalysis(selections(":*,*")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          columnName(":a") -> expr(":a"),
          columnName(":b") -> expr(":b"),
          columnName("c") -> expr("c"),
          columnName("d") -> expr("d"),
          columnName("e") -> expr("e")
        ),
        Seq(":a", ":b", "c", "d", "e").map(columnName)
      ))
    }

    "not select an alias the same as a column in the dataset context" in {
      AliasAnalysis(selections("(c)")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          columnName("c_1") -> expr("(c)")
        ),
        Seq("c_1").map(columnName)
      ))
    }

    "not select an alias the same as an expression in the dataset context" in {
      AliasAnalysis(selections("c as c_d, c - d")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          columnName("c_d") -> expr("c"),
          columnName("c_d_1") -> expr("c - d")
        ),
        Seq("c_d", "c_d_1").map(columnName)
      ))
    }

    "never infer the same alias twice" in {
      AliasAnalysis(selections("c + d, c - d")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          columnName("c_d") -> expr("c + d"),
          columnName("c_d_1") -> expr("c - d")
        ),
        Seq("c_d", "c_d_1").map(columnName)
      ))
    }

    "allow hiding a column if it's not selected" in {
      AliasAnalysis(selections("c as d")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          columnName("d") -> expr("c")
        ),
        Seq(columnName("d"))
      ))
    }

    "forbid hiding a column if it's selected via *" in {
      evaluating { AliasAnalysis(selections("*, c as d")) } must produce[DuplicateAliasException]
    }

    "forbid hiding a column if it's selected explicitly" in {
      evaluating { AliasAnalysis(selections("d, c as d")) } must produce[DuplicateAliasException]
    }

    "forbid hiding giving two expressions the same alias" in {
      evaluating { AliasAnalysis(selections("e as q, c as q")) } must produce[DuplicateAliasException]
    }

    "allow hiding a column if it's excluded via *" in {
      AliasAnalysis(selections("* (except d), c as d")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          columnName("c") -> expr("c"),
          columnName("d") -> expr("c"),
          columnName("e") -> expr("e")
        ),
        Seq("c", "e", "d").map(columnName)
      ))
    }

    "forbid excluding the same user column twice" in {
      evaluating { AliasAnalysis(selections("* (except c, c)")) } must produce[RepeatedExceptionException]
    }

    "forbid excluding the same system column twice" in {
      evaluating { AliasAnalysis(selections(":* (except :b, :b)")) } must produce[RepeatedExceptionException]
    }

    "forbid excluding a non-existant user column" in {
      evaluating { AliasAnalysis(selections("* (except gnu)")) } must produce[NoSuchColumnException]
    }

    "forbid excluding a non-existant system column" in {
      evaluating { AliasAnalysis(selections(":* (except :gnu)")) } must produce[NoSuchColumnException]
    }
  }
}
