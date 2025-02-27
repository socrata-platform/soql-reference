package com.socrata.soql.aliases

import scala.util.parsing.input.{NoPosition, Position}
import org.scalatest._
import org.scalatest.MustMatchers
import com.socrata.soql.parsing.{Lexer, LexerReader, Parser}
import com.socrata.soql.ast._
import com.socrata.soql.exceptions._
import com.socrata.soql.environment.{ColumnName, TableName, UntypedDatasetContext}
import com.socrata.soql.collection.{OrderedMap, OrderedSet}
import com.socrata.soql.parsing.AbstractParser.Parameters

import com.socrata.soql.aliases.AliasAnalysis.AliasType.{Implicit, SemiExplicit, Explicit}
import com.socrata.soql.aliases.AliasAnalysis.{ExpressionInfo, AliasType}

class AliasAnalysisTest extends WordSpec with MustMatchers {
  def columnNames(names: String*) =
    OrderedSet(names.map(ColumnName): _*)

  def fixtureContext(cols: String*) =
    Map(TableName.PrimaryTable.qualifier -> new UntypedDatasetContext {
        private implicit def dsc = this
        val locale = com.ibm.icu.util.ULocale.US
        lazy val columns = columnNames(cols: _*)
      })

  def fixturePosition(l: Int, c: Int): Position = new Position {
    def line = l
    def column = c
    def lineContents = " " * c
  }

  def se(e: String): SelectedExpression =
    SelectedExpression(expr(e), None)

  def se(e: String, name: String, position: Position): SelectedExpression =
    SelectedExpression(expr(e), Some((ColumnName(name), position)))

  implicit def selections(e: String): Selection = {
    new Parser().selection(e)
  }
  def selectionsNoPos(e: String): Selection = selections(e)
  def expr(e: String): Expression = {
    new Parser().expression(e)
  }
  def expr(e: String, aliasType: AliasType): ExpressionInfo = {
    ExpressionInfo(expr(e), aliasType)
  }
  def ident(e: String): ColumnName = {
    val p = new Parser()
    p.expression(e) match {
      case ColumnOrAliasRef(None, parsed) => parsed
      case failure => fail("Unable to parse expression fixture " + e + ": " + failure)
    }
  }
  def unaliased(names: String*)(pos: Position) = names.map { i => SelectedExpression(ColumnOrAliasRef(None, ColumnName(i))(pos), None) }

  "processing a star" should {
    implicit val ctx = fixtureContext()
    val pos = fixturePosition(4, 3)

    "expand to all input columns when there are no exceptions" in {
      // TODO: check the positions
      AliasAnalysis.processStar(StarSelection(None, Seq.empty).positionedAt(pos), columnNames("a","b","c")) must equal (unaliased("a", "b", "c")(pos))
    }

    "expand to the empty list when there are no columns" in {
      AliasAnalysis.processStar(StarSelection(None, Seq.empty).positionedAt(pos), columnNames()) must equal (Seq.empty)
    }

    "expand with exclusions exluded" in {
      // TODO: check the positions
      AliasAnalysis.processStar(StarSelection(None, Seq((ident("b"), fixturePosition(5, 3)))).positionedAt(pos), columnNames("a","b","c")) must equal (unaliased("a","c")(pos))
    }

    "throw an exception if an exception does not occur in the column-set" in {
      // TODO: Check the position
      a [NoSuchColumn] must be thrownBy { AliasAnalysis.processStar(StarSelection(None, Seq((ident("not_there"), NoPosition))).positionedAt(pos), columnNames("a","c")) }
    }

    "throw an exception if an exception occurs more than once" in {
        // TODO: Check the position
      a [RepeatedException] must be thrownBy { AliasAnalysis.processStar(StarSelection(None, Seq((ident("a"), NoPosition), (ident("a"), NoPosition))).positionedAt(pos), columnNames("a","c")) }
    }
  }

  "expanding a selection" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")
    val pos = fixturePosition(5, 12)
    val pos2 = fixturePosition(32, 1)
    val someSelections = selections("2+2,hello,avg(gnu) as average").expressions

    "return the input if there were no stars" in {
      AliasAnalysis.expandSelection(Selection(None, Seq.empty, someSelections)) must equal (someSelections)
    }

    "return the system columns if there was a :*" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(Some(StarSelection(None, Seq.empty).positionedAt(pos)), Seq.empty, someSelections)) must equal (unaliased(":a",":b")(pos) ++ someSelections)
    }

    "return the un-excepted columns if there was a :*" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(Some(StarSelection(None, Seq((ident(":a"), NoPosition))).positionedAt(pos)), Seq.empty, someSelections)) must equal (unaliased(":b")(pos) ++ someSelections)
    }

    "return the user columns if there was a *" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(None, Seq(StarSelection(None, Seq.empty).positionedAt(pos)), someSelections)) must equal (unaliased("c","d","e")(pos) ++ someSelections)
    }

    "return the un-excepted user columns if there was a *" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(None, Seq(StarSelection(None, Seq((ident("d"), NoPosition),(ident("e"), NoPosition))).positionedAt(pos)), someSelections)) must equal (unaliased("c")(pos) ++ someSelections)
    }

    "return the all user columns if there was a :* and a *" in {
      // TODO: check the positions
      AliasAnalysis.expandSelection(Selection(Some(StarSelection(None, Seq.empty).positionedAt(pos)), Seq(StarSelection(None, Seq.empty).positionedAt(pos2)), someSelections)) must equal (unaliased(":a",":b")(pos) ++ unaliased("c","d","e")(pos2) ++ someSelections)
    }
  }

  "assigning (semi-)explicit aliases" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")

    "assign aliases to simple columns" in {
      val ss = selections("2+2,hello,world,avg(gnu),(x)").expressions
      AliasAnalysis.assignExplicitAndSemiExplicit(ss) must equal (
        (Set(ColumnName("hello"),ColumnName("world")), Seq(ss(0), se("hello","hello", ss(1).expression.position), se("world", "world", ss(2).expression.position), ss(3), ss(4)))
      )
    }

    "accept aliases" in {
      val ss = selections("2+2 as four,hello as x,avg(gnu),world,(x)").expressions
      AliasAnalysis.assignExplicitAndSemiExplicit(ss) must equal (
        (Set(ColumnName("world")), Seq(ss(0), ss(1), ss(2), se("world","world",ss(3).expression.position), ss(4)))
      )
    }

    "reject duplicate aliases when one is explicit and the other semi-explicit" in {
      val ss = selections("2+2 as four, four").expressions
      a [DuplicateAlias] must be thrownBy { AliasAnalysis.assignExplicitAndSemiExplicit(ss) }
    }

    "reject duplicate aliases when both are semi-explicit" in {
      val ss = selections("four, four").expressions
      a [DuplicateAlias] must be thrownBy { AliasAnalysis.assignExplicitAndSemiExplicit(ss) }
    }

    "reject duplicate aliases when both are explicit" in {
      val ss = selections("2 + 2 as four, 1 + 3 as four").expressions
      a [DuplicateAlias] must be thrownBy { AliasAnalysis.assignExplicitAndSemiExplicit(ss) }
    }
  }

  "creating an implicit alias" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")

    "select a name" in {
      AliasAnalysis.implicitAlias(expr("a"), Set()) must equal (ColumnName("a"))
      AliasAnalysis.implicitAlias(expr("a + b"), Set()) must equal (ColumnName("a_b"))
    }

    "select a name not in use" in {
      AliasAnalysis.implicitAlias(expr("a + b"), columnNames("a_b")) must equal (ColumnName("a_b_1"))
      AliasAnalysis.implicitAlias(expr("c"), Set()) must equal (ColumnName("c_1")) // checking against the dataset context
      AliasAnalysis.implicitAlias(expr("a + b"), columnNames("a_b", "a_b_1", "a_b_2")) must equal (ColumnName("a_b_3"))
    }

    "not add unnecessary underscores when suffixing a disambiguator" in {
      AliasAnalysis.implicitAlias(expr("a + b_"), Set(ColumnName("a_b_"))) must equal (ColumnName("a_b_1"))
      AliasAnalysis.implicitAlias(expr("a + `b-`"), Set(ColumnName("a_b-"))) must equal (ColumnName("a_b-1"))
    }
  }

  "assigning implicits" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")

    "assign non-conflicting aliases" in {
      AliasAnalysis.assignImplicit(Seq(se(":a + 1", "c", NoPosition), se(":a + 1"), se("-c"), se("-d"), se("e", "d_1", NoPosition)), Set.empty) must equal (
        Map(
          ColumnName("c") -> expr(":a + 1", Explicit),
          ColumnName("a_1") -> expr(":a + 1", Implicit),
          ColumnName("c_1") -> expr("-c", Implicit),
          ColumnName("d_2") -> expr("-d", Implicit),
          ColumnName("d_1") -> expr("e", Explicit)
        )
      )
    }

    "preserve ordering" in {
      AliasAnalysis.assignImplicit(Seq(se(":a + 1", "c", NoPosition), se(":a + 1"), se("-c"), se("-d"), se("e", "d_1", NoPosition)), Set.empty).toSeq must equal (
        Seq(
          ColumnName("c") -> expr(":a + 1", Explicit),
          ColumnName("a_1") -> expr(":a + 1", Implicit),
          ColumnName("c_1") -> expr("-c", Implicit),
          ColumnName("d_2") -> expr("-d", Implicit),
          ColumnName("d_1") -> expr("e", Explicit)
        )
      )
    }

    "reject assigning to a straight identifier" in {
      an [AssertionError] must be thrownBy { AliasAnalysis.assignImplicit(Seq(se("q")), Set.empty) }
    }

    "count(*) is count" in {
      AliasAnalysis.assignImplicit(Seq(se("count(*)")), Set.empty) must equal (
        Map(
          ColumnName("count") -> expr("count(*)", Implicit)
        )
      )
    }

    "count(*) includes window options" in {
      AliasAnalysis.assignImplicit(Seq(se("count(*) over(partition by col)")), Set.empty) must equal (
        Map(
          ColumnName("count_over_partition_by_col") -> expr("count(*) over(partition by col)", Implicit)
        )
      )
    }
  }

  "ordering aliases for evaluation" should {
    implicit val ctx = fixtureContext()

    "accept semi-explicitly aliased names" in {
      AliasAnalysis.orderAliasesForEvaluation(OrderedMap(ColumnName("x") -> expr("x", SemiExplicit)), Set(ColumnName("x"))) must equal (Seq(ColumnName("x")))
    }

    "reject indirectly-circular aliases" in {
      val aliasMap = OrderedMap(
        ColumnName("x") -> expr("y * 2", Implicit),
        ColumnName("b") -> expr("x+1", Implicit),
        ColumnName("z") -> expr("5", Implicit),
        ColumnName("y") -> expr("a / b", Implicit)
      )
      a [CircularAliasDefinition] must be thrownBy { AliasAnalysis.orderAliasesForEvaluation(aliasMap, Set.empty) }
    }

    "produce a valid linearization" in {
      val aliasMap = OrderedMap(
        ColumnName("a") -> expr("b + c", Implicit),
        ColumnName("z") -> expr("5", Implicit),
        ColumnName("b") -> expr("7", Implicit),
        ColumnName("o") -> expr("b * d", Implicit),
        ColumnName("y") -> expr("a / b", Implicit),
        ColumnName("d") -> expr("sqrt(z)", Implicit)
      )
      val order = AliasAnalysis.orderAliasesForEvaluation(aliasMap, Set.empty)
      order.sorted must equal (Seq("a","b","d","o","y","z").map(ColumnName(_)))
      order.indexOf(ColumnName("y")) must be > (order.indexOf(ColumnName("a")))
      order.indexOf(ColumnName("y")) must be > (order.indexOf(ColumnName("b")))
      order.indexOf(ColumnName("a")) must be > (order.indexOf(ColumnName("b")))
      order.indexOf(ColumnName("d")) must be > (order.indexOf(ColumnName("z")))
      order.indexOf(ColumnName("o")) must be > (order.indexOf(ColumnName("b")))
      order.indexOf(ColumnName("o")) must be > (order.indexOf(ColumnName("d")))
    }
  }

  "the whole flow" should {
    implicit val ctx = fixtureContext(":a",":b","c","d","e")

    "expand *" in {
      AliasAnalysis(selections("*")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          ColumnName("c") -> expr("c", SemiExplicit),
          ColumnName("d") -> expr("d", SemiExplicit),
          ColumnName("e") -> expr("e", SemiExplicit)
        ),
        Seq("c","d","e").map(ColumnName)
      ))
    }

    "expand :*" in {
      AliasAnalysis(selections(":*")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          ColumnName(":a") -> expr(":a", SemiExplicit),
          ColumnName(":b") -> expr(":b", SemiExplicit)
        ),
        Seq(":a", ":b").map(ColumnName)
      ))
    }

    "expand :*,*" in {
      AliasAnalysis(selections(":*,*")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          ColumnName(":a") -> expr(":a", SemiExplicit),
          ColumnName(":b") -> expr(":b", SemiExplicit),
          ColumnName("c") -> expr("c", SemiExplicit),
          ColumnName("d") -> expr("d", SemiExplicit),
          ColumnName("e") -> expr("e", SemiExplicit)
        ),
        Seq(":a", ":b", "c", "d", "e").map(ColumnName)
      ))
    }

    "not select an alias the same as a column in the dataset context" in {
      AliasAnalysis(selections("(c)")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          ColumnName("c_1") -> expr("(c)", Implicit)
        ),
        Seq("c_1").map(ColumnName)
      ))
    }

    "not select an alias the same as an expression in the dataset context" in {
      AliasAnalysis(selections("c as c_d, c - d")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          ColumnName("c_d") -> expr("c", Explicit),
          ColumnName("c_d_1") -> expr("c - d", Implicit)
        ),
        Seq("c_d", "c_d_1").map(ColumnName)
      ))
    }

    "never infer the same alias twice" in {
      AliasAnalysis(selections("c + d, c - d")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          ColumnName("c_d") -> expr("c + d", Implicit),
          ColumnName("c_d_1") -> expr("c - d", Implicit)
        ),
        Seq("c_d", "c_d_1").map(ColumnName)
      ))
    }

    "allow hiding a column if it's not selected" in {
      AliasAnalysis(selections("c as d")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          ColumnName("d") -> expr("c", Explicit)
        ),
        Seq(ColumnName("d"))
      ))
    }

    "forbid hiding a column if it's selected via *" in {
      a [DuplicateAlias] must be thrownBy { AliasAnalysis(selections("*, c as d")) }
    }

    "forbid hiding a column if it's selected explicitly" in {
      a [DuplicateAlias] must be thrownBy { AliasAnalysis(selections("d, c as d")) }
    }

    "forbid hiding giving two expressions the same alias" in {
      a [DuplicateAlias] must be thrownBy { AliasAnalysis(selections("e as q, c as q")) }
    }

    "allow hiding a column if it's excluded via *" in {
      AliasAnalysis(selections("* (except d), c as d")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          ColumnName("c") -> expr("c", SemiExplicit),
          ColumnName("d") -> expr("c", Explicit),
          ColumnName("e") -> expr("e", SemiExplicit)
        ),
        Seq("c", "e", "d").map(ColumnName)
      ))
    }

    "forbid excluding the same user column twice" in {
      a [RepeatedException] must be thrownBy { AliasAnalysis(selections("* (except c, c)")) }
    }

    "forbid excluding the same system column twice" in {
      a [RepeatedException] must be thrownBy { AliasAnalysis(selections(":* (except :b, :b)")) }
    }

    "forbid excluding a non-existant user column" in {
      a [NoSuchColumn] must be thrownBy { AliasAnalysis(selections("* (except gnu)")) }
    }

    "forbid excluding a non-existant system column" in {
      a [NoSuchColumn] must be thrownBy { AliasAnalysis(selections(":* (except :gnu)")) }
    }

    "allow standard system columns in aliases" in {
      val parser = new Parser(new Parameters(true, Set(":id", ":created_at", ":updated_at").map(ColumnName(_))))
      AliasAnalysis(parser.selection("max(:id) as :id, max(:created_at) as :created_at, max(:updated_at) as :updated_at")) must equal(AliasAnalysis.Analysis(
        OrderedMap(
          ColumnName(":id") -> expr("max(:id)", Explicit),
          ColumnName(":created_at") -> expr("max(:created_at)", Explicit),
          ColumnName(":updated_at") -> expr("max(:updated_at)", Explicit)
        ),
        Seq(":id", ":created_at", ":updated_at").map(ColumnName)
      ))
    }

    "forbid aliases starting with colon and not a standard system column" in {
      a [BadParse] must be thrownBy { AliasAnalysis(selections(":id as :other")) }
    }
  }
}
