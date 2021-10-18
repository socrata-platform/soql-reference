package com.socrata.soql.parsing

import scala.collection.immutable.ListSet
import scala.collection.compat.immutable.LazyList
import scala.util.parsing.input.Position
import scala.annotation.tailrec

import com.socrata.NonEmptySeq
import com.socrata.soql.ast._
import com.socrata.soql.tokens._
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery, ast, tokens}
import com.socrata.soql.environment.{ColumnName, FunctionName, HoleName, TableName, TypeName}

object RecursiveDescentParser {
  private class Keyword(s: String) {
    def apply() = Identifier(s, false)
    def unapply(ident: Identifier): Boolean = {
      !ident.quoted && ident.value.equalsIgnoreCase(s)
    }
  }

  sealed abstract class Expectation {
    def printable: String
  }
  case class ActualToken(token: Token) extends Expectation {
    def printable = token.quotedPrintable
  }
  case object AnIntegerLiteral extends Expectation {
    def printable = "an integer"
  }
  case object AStringLiteral extends Expectation {
    def printable = "a string"
  }
  case object AnIdentifier extends Expectation {
    // This is used both where we expect only a user identifier and
    // where we expect either a user or a system identifier.  If the
    // user provides a system id where we're not expecting it, _then_
    // we'll specify a non-system identifier.
    def printable = "an identifier"
  }
  case object ASystemIdentifier extends Expectation {
    def printable = "a system identifier"
  }
  case object ANonSystemIdentifier extends Expectation {
    def printable = "a non-system identifier"
  }
  case object ATableIdentifier extends Expectation {
    def printable = "a table identifier"
  }
  case object AnExpression extends Expectation {
    def printable = "an expression"
  }
  case object AGroupBy extends Expectation {
    def printable = "`GROUP BY'"
  }
  case object AnOrderBy extends Expectation {
    def printable = "`ORDER BY'"
  }
  case object APartitionBy extends Expectation {
    def printable = "`PARTITION BY'"
  }
  case object AnIsNot extends Expectation {
    def printable = "`IS NOT'"
  }
  case object ANotLike extends Expectation {
    def printable = "`NOT LIKE'"
  }
  case object ANotBetween extends Expectation {
    def printable = "`NOT BETWEEN'"
  }
  case object ANotIn extends Expectation {
    def printable = "`NOT IN'"
  }
  case object AnAliasForThis extends Expectation {
    def printable = "an alias for `@this'"
  }
  case object AnOperator extends Expectation {
    def printable = "an operator"
  }

  private implicit def tokenAsTokenLike(t: Token): Expectation = ActualToken(t)

  def expectationsToEnglish(expectation: Iterable[Expectation], got: Token): String = {
    val sb = new StringBuilder
    sb.append("Expected")

    def loop(expectations: LazyList[RecursiveDescentParser.Expectation], n: Int): Unit = {
      expectations match {
        case LazyList() =>
          // Uhhh... this shouldn't happen
          sb.append(" nothing")
        case LazyList.cons(hd, LazyList()) =>
          if(n == 1) sb.append(" or ")
          else if(n > 1) sb.append(", or ") // oxford comma 4eva
          else sb.append(' ')
          sb.append(hd.printable)
        case LazyList.cons(hd, tl) =>
          if(n == 0) sb.append(" one of ")
          else sb.append(", ")
          sb.append(hd.printable)
          loop(tl, n+1)
      }
    }
    loop(expectation.to(LazyList), 0)

    sb.append(", but got ").
      append(got.quotedPrintable).
      toString
  }

  private val CASE = new Keyword("CASE")
  private val WHEN = new Keyword("WHEN")
  private val THEN = new Keyword("THEN")
  private val ELSE = new Keyword("ELSE")
  private val END = new Keyword("END")
  private val OVER = new Keyword("OVER")
  private val PARTITION = new Keyword("PARTITION")
  private val BY = new Keyword("BY")
  private val NULLS = new Keyword("NULLS")
  private val FIRST = new Keyword("FIRST")
  private val LAST = new Keyword("LAST")
  private val RANGE = new Keyword("RANGE")
  private val ROWS = new Keyword("ROWS")
  private val UNBOUNDED = new Keyword("UNBOUNDED")
  private val PRECEDING = new Keyword("PRECEDING")
  private val CURRENT = new Keyword("CURRENT")
  private val ROW = new Keyword("ROW")
  private val FOLLOWING = new Keyword("FOLLOWING")

  private sealed abstract class NullPlacement
  private case object First extends NullPlacement
  private case object Last extends NullPlacement

  type Reader = ExtendedReader[Token]

  trait ParseException extends Exception {
    def position: Position = reader.first.position
    val reader: Reader
  }

  private case class ParseResult[+T](reader: Reader, value: T) {
    def map[U](f: T=>U): ParseResult[U] = copy(value = f(value))
  }

  // sigh... ut's unfortunate that these are all here rather than
  // being close to the expectations they reflect, but alas Scala
  // provides no way to create a nontrivial immutable value and have
  // it cached other than by sticking it in a `val` on some object,
  // and recreating these sets on-site has a fairly hefty runtime
  // cost.
  private[this] def s(xs: Expectation*) = ListSet(xs : _*)
  private val ANDSET = s(AND())
  private val LIMITOFFSETSET = s(LIMIT(), OFFSET())
  private val LIMITSET = s(LIMIT())
  private val OFFSETSET = s(OFFSET())
  private val ORDERBYSEARCHSET = s(AnOrderBy, SEARCH())
  private val ORDERBYSET = s(AnOrderBy)
  private val SEARCHSET = s(SEARCH())
  private val GROUPBYSET = s(AGroupBy)
  private val WHERESET = s(WHERE())
  private val HAVINGSET = s(HAVING())
  private val TABLEIDCOLONSTARSET = s(ATableIdentifier, COLONSTAR())
  private val LPARENSET = s(LPAREN())
  private val COMMASET = s(COMMA())
  private val TABLEIDSTARSET = s(ATableIdentifier, STAR())
  private val ASSET = s(AS())
  private val EXPRESSIONSET = s(AnExpression)
  private val JOINSET = s(LEFT(), RIGHT(), FULL(), JOIN())
  private val ASLPARENSET = s(AS(), LPAREN())
  private val TABLEIDSET = s(ATableIdentifier)
  private val QUERYPIPESET = s(QUERYPIPE())
  private val ROWOPSET = s(QUERYPIPE(), QUERYUNION(), QUERYINTERSECT(), QUERYMINUS(), QUERYUNIONALL(), QUERYINTERSECTALL(), QUERYMINUSALL())
  private val DISTINCTSET = s(DISTINCT())
  private val FROMSET = s(FROM())
  private val PARTITIONBYSET = s(APartitionBy)
  private val ASCDESCSET = s(ASC(), DESC())
  private val NULLNULLSSET = s(NULL(), NULLS())
  private val RANGEROWSSET = s(RANGE(), ROWS())
  private val OVERSET = s(OVER())
  private val DOTLBRACKETSET = s(DOT(), LBRACKET())

  // These are all collapsed into "an operator" for ease of
  // interpretation by an end-user instead of getting a bunch of
  // line-noise.  Leaving them here in case we want to bring them back
  // for some reason...

  // private val COLONCOLONSET = s(COLONCOLON())
  // private val MINUSPLUSSET = s(MINUS(), PLUS())
  // private val CARETSET = s(CARET())
  // private val FACTORSET = s(STAR(), SLASH(), PERCENT())
  // private val TERMSET = s(PLUS(), MINUS(), PIPEPIPE())
  // private val COMPARISONSET = s(EQUALS(), LESSGREATER(), LESSTHAN(), LESSTHANOREQUALS(), GREATERTHAN(), GREATERTHANOREQUALS(), EQUALSEQUALS(), BANGEQUALS())
  private val OPERATORSET = s(AnOperator)

  private val LIKEBETWEENINSET = s(IS(), AnIsNot, LIKE(), ANotLike, BETWEEN(), ANotBetween, IN(), ANotIn)
  private val NOTSET = s(NOT())
  private val ORSET = s(OR())
}

abstract class RecursiveDescentParser(parameters: AbstractParser.Parameters = AbstractParser.defaultParameters) extends AbstractParser {
  import parameters._
  import RecursiveDescentParser._

  // These are the things that need implementing
  protected def lexer(s: String): AbstractLexer
  protected def expected(reader: Reader): ParseException
  protected def expectedLeafQuery(reader: Reader): ParseException

  def binaryTreeSelect(soql: String): BinaryTree[Select] = parseFull(compoundSelect, soql)

  def selection(soql: String): Selection = parseFull(selectList, soql)
  def joins(soql: String): Seq[Join] = parseFull(joinList, soql)
  def expression(soql: String): Expression = parseFull(topLevelExpr, soql)
  def orderings(soql: String): Seq[OrderBy] = parseFull(orderingList, soql)
  def groupBys(soql: String): Seq[Expression] = parseFull(commaSeparatedExprs, soql)

  def selectStatement(soql: String): NonEmptySeq[Select] = parseFull(pipedSelect, soql)
  def unchainedSelectStatement(soql: String): Select = parseFull(select, soql)
  def parseJoinSelect(soql: String): JoinSelect = parseFull(joinSelect, soql)
  def limit(soql: String): BigInt = parseFull(integerLiteral, soql)
  def offset(soql: String): BigInt = parseFull(integerLiteral, soql)
  def search(soql: String): String = parseFull(stringLiteral, soql)

  private def fail(reader: Reader, expectations: Set[Expectation]): Nothing = {
    reader.addAlternates(expectations)
    throw expected(reader)
  }
  private def fail1(reader: Reader, expectation: Expectation): Nothing = {
    fail(reader, Set(expectation))
  }

  private def parseFull[T](parser: Reader => ParseResult[T], soql: String): T = {
    val ParseResult(end, result) = parser(new LexerReader(lexer(soql)))
    if(end.first.isInstanceOf[EOF]) {
      result
    } else {
      fail1(end, EOF())
    }
  }

  private def integerLiteral(reader: Reader): ParseResult[BigInt] = {
    reader.first match {
      case n: IntegerLiteral =>
        ParseResult(reader.rest, n.asInt)
      case _ =>
        fail1(reader, AnIntegerLiteral)
    }
  }

  private def stringLiteral(reader: Reader): ParseResult[String] = {
    reader.first match {
      case s: tokens.StringLiteral =>
        ParseResult(reader.rest, s.value)
      case _ =>
        fail1(reader, AStringLiteral)
    }
  }

  private def select(reader: Reader): ParseResult[Select] = {
    reader.first match {
      case SELECT() =>
        val ParseResult(r2, d) = distinct(reader.rest)
        val ParseResult(r3, selected) = selectList(r2)
        val ParseResult(r4, fromClause) = from(r3)
        val ParseResult(r5, joinClause) = if(allowJoins) joinList(r4) else ParseResult(r4, Seq.empty)
        val ParseResult(r6, whereClause) = where(r5)
        val ParseResult(r7, groupByClause) = groupBy(r6)
        val ParseResult(r8, havingClause) = having(r7)
        val ParseResult(r9, (orderByClause, searchClause)) = orderByAndSearch(r8)
        val ParseResult(r10, (limitClause, offsetClause)) = limitOffset(r9)
        ParseResult(r10, Select(d, selected, fromClause, joinClause, whereClause, groupByClause, havingClause, orderByClause, limitClause, offsetClause, searchClause))
      case _ =>
        fail1(reader, SELECT())
    }
  }

  private def limitOffset(reader: Reader): ParseResult[(Option[BigInt], Option[BigInt])] = {
    reader.first match {
      case LIMIT() =>
        val ParseResult(r2, limitClause) = limit(reader)
        val ParseResult(r3, offsetClause) = offset(r2)
        ParseResult(r3, (limitClause, offsetClause))
      case OFFSET() =>
        val ParseResult(r2, offsetClause) = offset(reader)
        val ParseResult(r3, limitClause) = limit(r2)
        ParseResult(r3, (limitClause, offsetClause))
      case _ =>
        reader.addAlternates(LIMITOFFSETSET)
        ParseResult(reader, (None, None))
    }
  }

  private def limit(reader: Reader): ParseResult[Option[BigInt]] = {
    reader.first match {
      case LIMIT() =>
        reader.rest.first match {
          case lit: IntegerLiteral => ParseResult(reader.rest.rest, Some(lit.asInt))
          case _ => fail1(reader.rest, AnIntegerLiteral)
        }
      case _ =>
        reader.addAlternates(LIMITSET)
        ParseResult(reader, None)
    }
  }


  private def offset(reader: Reader): ParseResult[Option[BigInt]] = {
    reader.first match {
      case OFFSET() =>
        reader.rest.first match {
          case lit: IntegerLiteral => ParseResult(reader.rest.rest, Some(lit.asInt))
          case _ => fail1(reader.rest, AnIntegerLiteral)
        }
      case _ =>
        reader.addAlternates(OFFSETSET)
        ParseResult(reader, None)
    }
  }

  private def orderByAndSearch(reader: Reader): ParseResult[(Seq[OrderBy], Option[String])] = {
    reader.first match {
      case ORDER() =>
        val ParseResult(r2, orderByClause) = orderBy(reader)
        val ParseResult(r3, searchClause) = search(r2)
        ParseResult(r3, (orderByClause, searchClause))
      case SEARCH() =>
        val ParseResult(r2, searchClause) = search(reader)
        val ParseResult(r3, orderByClause) = orderBy(r2)
        ParseResult(r3, (orderByClause, searchClause))
      case _ =>
        reader.addAlternates(ORDERBYSEARCHSET)
        ParseResult(reader, (Nil, None))
    }
  }

  private def orderBy(reader: Reader): ParseResult[Seq[OrderBy]] = {
    reader.first match {
      case ORDER() =>
        reader.rest.first match {
          case BY() =>
            orderingList(reader.rest.rest)
          case _ =>
            fail1(reader.rest, BY())
        }
      case _ =>
        reader.addAlternates(ORDERBYSET)
        ParseResult(reader, Nil)
    }
  }

  private def search(reader: Reader): ParseResult[Option[String]] = {
    reader.first match {
      case SEARCH() =>
        reader.rest.first match {
          case tokens.StringLiteral(s) =>
            ParseResult(reader.rest.rest, Some(s))
          case _ =>
            fail1(reader.rest, AStringLiteral)
        }
      case _ =>
        reader.addAlternates(SEARCHSET)
        ParseResult(reader, None)
    }
  }

  private def groupBy(reader: Reader): ParseResult[Seq[Expression]] = {
    reader.first match {
      case GROUP() =>
        reader.rest.first match {
          case BY() =>
            val r = commaSeparatedExprs(reader.rest.rest)
            r.reader.resetAlternates()
            r
          case _ =>
            fail1(reader, BY())
        }
      case _ =>
        reader.addAlternates(GROUPBYSET)
        ParseResult(reader, Nil)
    }
  }

  private def where(reader: Reader): ParseResult[Option[Expression]] = {
    reader.first match {
      case WHERE() =>
        topLevelExpr(reader.rest).map(Some(_))
      case _ =>
        reader.addAlternates(WHERESET)
        ParseResult(reader, None)
    }
  }

  private def having(reader: Reader): ParseResult[Option[Expression]] = {
    reader.first match {
      case HAVING() =>
        topLevelExpr(reader.rest).map(Some(_))
      case _ =>
        reader.addAlternates(HAVINGSET)
        ParseResult(reader, None)
    }
  }

  private def selectList(reader: Reader): ParseResult[Selection] = {
    val ParseResult(r2, systemStar) = systemStarSelection(reader)
    val ParseResult(r3, userStars) = userStarSelections(r2, systemStar.isDefined)
    val ParseResult(r4, expressions) = expressionSelectList(r3, systemStar.isDefined || userStars.nonEmpty)
    ParseResult(r4, Selection(systemStar, userStars, expressions))
  }

  private def systemStarSelection(reader: Reader): ParseResult[Option[StarSelection]] = {
    reader.first match {
      case ti: TableIdentifier =>
        reader.rest.first match {
          case DOT() =>
            reader.rest.rest.first match {
              case star@COLONSTAR() =>
                selectExceptions(reader.rest.rest.rest, systemIdentifier).map { exceptions =>
                  Some(StarSelection(Some(intoQualifier(ti)), exceptions).positionedAt(star.position))
                }
              case _ =>
                // whoops, we're not parsing a :* at all, bail out
                reader.addAlternates(TABLEIDCOLONSTARSET)
                ParseResult(reader, None)
            }
          case _ =>
            fail1(reader.rest, DOT())
        }
      case star@COLONSTAR() =>
        selectExceptions(reader.rest, systemIdentifier).map { exceptions =>
          Some(StarSelection(None, exceptions).positionedAt(star.position))
        }
      case _ =>
        reader.addAlternates(TABLEIDCOLONSTARSET)
        ParseResult(reader, None)
    }
  }

  private def selectExceptions(reader: Reader, identParser: Reader => ParseResult[(ColumnName, Position)]): ParseResult[Seq[(ColumnName, Position)]] = {
    reader.first match {
      case LPAREN() =>
        reader.rest.first match {
          case EXCEPT() =>
            val result = Vector.newBuilder[(ColumnName, Position)]
            @tailrec
            def loop(reader: Reader): Reader = {
              val ParseResult(r2, ident) = identParser(reader)
              result += ident
              r2.first match {
                case COMMA() =>
                  loop(r2.rest)
                case RPAREN() =>
                  r2.rest
                case _ =>
                  fail(r2, Set(COMMA(), RPAREN()))
              }
            }
            val finalReader = loop(reader.rest.rest)
            ParseResult(finalReader, result.result())
          case _ =>
            fail1(reader.rest, EXCEPT())
        }
      case _ =>
        reader.addAlternates(LPARENSET)
        ParseResult(reader, Nil)
    }
  }

  private def systemIdentifier(reader: Reader): ParseResult[(ColumnName, Position)] = {
    reader.first match {
      case si: SystemIdentifier =>
        ParseResult(reader.rest, (ColumnName(si.value), si.position))
      case _ =>
        fail1(reader, ASystemIdentifier)
    }
  }

  private def simpleIdentifier(reader: Reader): ParseResult[(String, Position)] =
    reader.first match {
      case i: Identifier => ParseResult(reader.rest, (i.value, i.position))
      case _ : SystemIdentifier => fail1(reader, ANonSystemIdentifier)
      case _ => fail1(reader, AnIdentifier)
    }

  private def userIdentifier(reader: Reader): ParseResult[(ColumnName, Position)] = {
    simpleIdentifier(reader).map { case (n, p) => (ColumnName(n), p) }
  }

  private def identifier(reader: Reader): ParseResult[(ColumnName, Position)] = {
    reader.first match {
      case i: Identifier =>
        ParseResult(reader.rest, (ColumnName(i.value), i.position))
      case si: SystemIdentifier =>
        ParseResult(reader.rest, (ColumnName(si.value), si.position))
      case _ =>
        fail1(reader, AnIdentifier)
    }
  }

  private def userStarSelections(reader: Reader, commaFirst: Boolean): ParseResult[Seq[StarSelection]] = {
    // This is a little tricky, because we may or may not be parsing
    // anything at all, so we need to keep track of when we've
    // committed to a thing.  Basically we're parsing either
    //     (COMMA userStarExpr)*
    // if commaFirst is true and
    //     (userStarExpr (COMMA userStarExpr)*)?
    // if it's false, and we're only committed to completing a phrase
    // at the points where we've actually seen a STAR token

    val result = Vector.newBuilder[StarSelection]

    @tailrec
    def loop(reader: Reader, commaFirst: Boolean): Reader = {
      val r =
        if(commaFirst) {
          reader.first match {
            case COMMA() =>
              reader.rest
            case _ =>
              // We require a comma but do not see one - revert.
              reader.addAlternates(COMMASET)
              return reader
          }
        } else {
          reader
        }

      r.first match {
        case ti: TableIdentifier =>
          r.rest.first match {
            case DOT() =>
              r.rest.rest.first match {
                case star@STAR() =>
                  // We are now committed to parsing a user-star-selection
                  val ParseResult(r2, exceptions) = selectExceptions(r.rest.rest.rest, userIdentifier)
                  result += StarSelection(Some(intoQualifier(ti)), exceptions).positionedAt(star.position)
                  loop(r2, commaFirst = true)
                case _ =>
                  // Whoops, not a star selection, revert
                  reader.addAlternates(TABLEIDSTARSET)
                  reader
              }
            case _ =>
              reader.addAlternates(TABLEIDSTARSET)
              reader
          }
        case star@STAR() =>
          val ParseResult(r2, exceptions) = selectExceptions(r.rest, userIdentifier)
          result += StarSelection(None, exceptions).positionedAt(star.position)
          loop(r2, commaFirst = true)
        case _ =>
          reader.addAlternates(TABLEIDSTARSET)
          reader
      }
    }

    val finalReader = loop(reader, commaFirst)
    ParseResult(finalReader, result.result())
  }

  private def expressionSelectList(reader: Reader, commaFirst: Boolean): ParseResult[Seq[SelectedExpression]] = {
    // similar to the user select list, we need to track our commits
    val result = Vector.newBuilder[SelectedExpression]

    @tailrec
    def loop(reader: Reader, commaFirst: Boolean): Reader = {
      // Unlike the user select list above, if we need to see a comma
      // and we see one, we're committed to parsing an expression.
      // Otherwise we might just be at the end of the list!

      val r =
        if(commaFirst) {
          reader.first match {
            case COMMA() =>
              reader.rest
            case _ =>
              // We require a comma but do not see one - revert.
              reader.addAlternates(COMMASET)
              return reader
          }
        } else {
          reader
        }

      // This is written in a slightly strange way to placate the
      // tail-recursion checker (it's still lexically tail-recursive
      // if the `catch` does a `return reader` but apparently that
      // does something internally which causes it not to be)

      val exprResult =
        try {
          Some(topLevelExpr(r))
        } catch {
          case e: ParseException if !commaFirst && e.reader == r =>
            // ok, so if the parse failed right at the start we'll
            // assume we're good at the end of the select list and are
            // good to move on...
            None
        }

      exprResult match {
        case Some(ParseResult(r2, e)) =>
          r2.first match {
            case AS() =>
              // now we must see an identifier...
              val ParseResult(r3, identPos) = alias(r2.rest)
              result += SelectedExpression(e, Some(identPos))
              loop(r3, commaFirst = true)
            case _ =>
              r2.addAlternates(ASSET)
              result += SelectedExpression(e, None)
              loop(r2, commaFirst = true)
          }
        case None =>
          reader.addAlternates(EXPRESSIONSET)
          reader
      }
    }

    val finalReader = loop(reader, commaFirst)
    ParseResult(finalReader, result.result())
  }

  private def alias(reader: Reader): ParseResult[(ColumnName, Position)] = {
    reader.first match {
      case i: Identifier =>
        ParseResult(reader.rest, (ColumnName(i.value), i.position))
      case si: SystemIdentifier =>
        val cn = ColumnName(si.value)
        if(parameters.systemColumnAliasesAllowed.contains(cn)) {
          ParseResult(reader.rest, (cn, si.position))
        } else {
          fail1(reader, ANonSystemIdentifier)
        }
      case _ =>
        fail1(reader, AnIdentifier)
    }
  }

  private def joinList(reader: Reader): ParseResult[Seq[Join]] = {
    val acc = Vector.newBuilder[Join]

    @tailrec
    def loop(reader: Reader): Reader = {
      val ParseResult(r2, joinClause) = join(reader)
      joinClause match {
        case Some(j) =>
          acc += j
          loop(r2)
        case None =>
          r2
      }
    }

    val finalReader = loop(reader)
    ParseResult(finalReader, acc.result())
  }

  private def join(reader: Reader): ParseResult[Option[Join]] = {
    reader.first match {
      case direction@(LEFT() | RIGHT() | FULL()) =>
        val r2 = reader.rest.first match {
          case OUTER() => reader.rest.rest
          case _ => reader.rest
        }
        r2.first match {
          case JOIN() =>
            finishJoin(r2.rest, OuterJoin(direction, _, _, _)).map(Some(_))
          case _ =>
            fail1(reader, JOIN())
        }
      case JOIN() =>
        finishJoin(reader.rest, InnerJoin(_, _, _)).map(Some(_))
      case _ =>
        reader.addAlternates(JOINSET)
        ParseResult(reader, None)
    }
  }

  private def finishJoin(reader: Reader, join: (JoinSelect, Expression, Boolean) => Join): ParseResult[Join] = {
    val (r2, isLateral) = reader.first match {
      case LATERAL() => (reader.rest, true)
      case _ => (reader, false)
    }
    val ParseResult(r3, select) = joinSelect(r2)
    r3.first match {
      case ON() =>
        topLevelExpr(r3.rest).map(join(select, _, isLateral))
      case _ =>
        fail1(r3, ON())
    }
  }

  private def joinSelect(reader: Reader): ParseResult[JoinSelect] = {
    reader.first match {
      case ti: TableIdentifier =>
        // could be JoinTable, could be JoinFunc if that's allowed here
        reader.rest.first match {
          case AS() =>
            // JoinTable
            tableAlias(reader.rest.rest).map { a =>
              JoinTable(TableName(intoQualifier(ti), Some(a)))
            }
          case LPAREN() if allowJoinFunctions =>
            // JoinFunc
            val ParseResult(r2, args) = parseArgList(reader.rest.rest)
            r2.first match {
              case AS() =>
                tableAlias(r2.rest).map { a =>
                  JoinFunc(TableName(intoQualifier(ti), Some(a)), args)(ti.position)
                }
              case _ =>
                r2.addAlternates(ASSET)
                ParseResult(r2, JoinFunc(TableName(intoQualifier(ti), None), args)(ti.position))
            }
          case _ =>
            // Also JoinTable
            reader.rest.addAlternates(ASLPARENSET)
            ParseResult(reader.rest, JoinTable(TableName(intoQualifier(ti), None)))
        }
      case _ =>
        reader.addAlternates(TABLEIDSET)
        val ParseResult(r2, s) = atomSelect(reader)
        r2.first match {
          case AS() =>
            tableAlias(r2.rest).map(JoinQuery(s, _))
          case _ =>
            fail1(r2, AS())
        }
    }
  }

  private def atomSelect(reader: Reader): ParseResult[BinaryTree[Select]] = {
    reader.first match {
      case LPAREN() =>
        parenSelect(reader.rest)
      case _ =>
        reader.addAlternates(LPARENSET)
        select(reader).map(Leaf(_))
    }
  }

  // already past the lparen
  private def parenSelect(reader: Reader): ParseResult[BinaryTree[Select]] = {
    val ParseResult(r2, s) = compoundSelect(reader)
    r2.first match {
      case RPAREN() =>
        ParseResult(r2.rest, s)
      case _ =>
        fail1(r2, RPAREN())
    }
  }

  private def pipedSelect(reader: Reader): ParseResult[NonEmptySeq[Select]] = {
    val ParseResult(r2, s1) = select(reader)
    val tail = Vector.newBuilder[Select]

    @tailrec
    def loop(reader: Reader): Reader = {
      reader.first match {
        case QUERYPIPE() =>
          val ParseResult(r2, s) = select(reader.rest)
          tail += s
          loop(r2)
        case _ =>
          reader.addAlternates(QUERYPIPESET)
          reader
      }
    }
    val finalReader = loop(r2)

    ParseResult(finalReader, NonEmptySeq(s1, tail.result()))
  }

  private def compoundSelect(reader: Reader): ParseResult[BinaryTree[Select]] = {
    val ParseResult(r2, s) = atomSelect(reader)
    compoundSelect_!(r2, s)
  }

  @tailrec
  private def compoundSelect_!(reader: Reader, arg: BinaryTree[Select]): ParseResult[BinaryTree[Select]] = {
    reader.first match {
      case QUERYPIPE() =>
        val ParseResult(r2, arg2) = atomSelect(reader.rest)
        arg2.asLeaf match {
          case Some(leaf) =>
            compoundSelect_!(r2, PipeQuery(arg, Leaf(leaf)))
          case None =>
            // We need arg2 to be a leaf query, and we got a compound
            // query, so reset any alternate expectations we have and
            // fail with that specific error.
            reader.rest.resetAlternates()
            throw expectedLeafQuery(reader.rest)
        }
      case op@(QUERYUNION() | QUERYINTERSECT() | QUERYMINUS() | QUERYUNIONALL() | QUERYINTERSECTALL() | QUERYMINUSALL()) =>
        val ParseResult(r2, arg2) = atomSelect(reader.rest)
        compoundSelect_!(r2, Compound(op.printable, arg, arg2))
      case _ =>
        reader.addAlternates(ROWOPSET)
        ParseResult(reader, arg)
    }
  }

  private def distinct(reader: Reader): ParseResult[Boolean] = {
    reader.first match {
      case DISTINCT() =>
        ParseResult(reader.rest, true)
      case _ =>
        reader.addAlternates(DISTINCTSET)
        ParseResult(reader, false)
    }
  }

  private def from(reader: Reader): ParseResult[Option[TableName]] = {
    reader.first match {
      case FROM() =>
        // sadness: this loses position information
        val tn =
          reader.rest.first match {
            case tn: TableIdentifier => intoQualifier(tn)
            case _ => fail1(reader.rest, ATableIdentifier)
          }
        val ParseResult(r2, alias) =
          reader.rest.rest.first match {
            case AS() =>
              tableAlias(reader.rest.rest.rest).map(Some(_))
            case _ =>
              ParseResult(reader.rest.rest, None)
          }
        val tableName = TableName(tn, alias)
        if(tableName.nameWithSodaFountainPrefix == TableName.This && tableName.alias.isEmpty) {
          // We need an alias, so report an error at the point where
          // we did NOT see the AS token.
          reader.rest.rest.resetAlternates()
          fail1(reader.rest.rest, AnAliasForThis)
        }
        ParseResult(r2, Some(tableName))
      case _ =>
        reader.addAlternates(FROMSET)
        ParseResult(reader, None)
    }
  }

  private def tableAlias(reader: Reader): ParseResult[String] = {
    val ta =
      reader.first match {
        case ident: Identifier => ident.value
        case ident: TableIdentifier => intoQualifier(ident)
        case _ => fail1(reader, ATableIdentifier) // This is a lie because we also accept an unadorned identifier, but that's legacy syntax
      }
    ParseResult(reader.rest, TableName.withSodaFountainPrefix(ta))
  }

  private def conditional(reader: Reader, casePos: Position): ParseResult[Expression] = {
    // we're given a reader that's already past the CASE, so we just
    // need to parse (WHEN expr THEN expr)+ ELSE expr END
    val cases = Vector.newBuilder[Expression]

    @tailrec
    def loop(reader: Reader): Either[Reader, Reader] = {
      // this is in a reader that has consumed the WHEN and therefore is reading a condition
      val ParseResult(r2, condition) = nestedExpr(reader)
      r2.first match {
        case THEN() =>
          val ParseResult(r3, consequent) = nestedExpr(r2.rest)
          cases += condition
          cases += consequent
          r3.first match {
            case WHEN() =>
              loop(r3.rest)
            case ELSE() =>
              Left(r3.rest)
            case END() =>
              Right(r3.rest)
            case _ =>
              fail(r3, Set(WHEN(), ELSE(), END()))
          }
        case _ =>
          fail1(r2, THEN())
      }
    }

    reader.first match {
      case WHEN() =>
        val finalReader =
          loop(reader.rest) match {
            case Left(elseReader) =>
              val ParseResult(r2, alternate) = nestedExpr(elseReader)
              cases += ast.BooleanLiteral(true)(casePos)
              cases += alternate
              r2.first match {
                case END() => r2.rest
                case _ => fail1(r2, END())
              }
            case Right(endReader) =>
              cases += ast.BooleanLiteral(false)(casePos)
              cases += ast.NullLiteral()(casePos)
              endReader
          }
        ParseResult(finalReader, FunctionCall(SpecialFunctions.Case, cases.result(), None)(casePos, casePos))
      case _ =>
        fail1(reader, WHEN())
    }
  }

  private def commaSeparatedExprs(reader: Reader): ParseResult[Seq[Expression]] = {
    val args = Vector.newBuilder[Expression]

    @tailrec
    def loop(reader: Reader): Reader = {
      val ParseResult(r2, candidate) = nestedExpr(reader)
      args += candidate
      r2.first match {
        case COMMA() =>
          loop(r2.rest)
        case _ =>
          r2.addAlternates(COMMASET)
          r2
      }
    }
    val finalReader = loop(reader)

    ParseResult(finalReader, args.result())
  }

  private def windowPartitionBy(reader: Reader): ParseResult[Seq[Expression]] = {
    reader.first match {
      case PARTITION() =>
        reader.rest.first match {
          case BY() => commaSeparatedExprs(reader.rest.rest)
          case _ => fail1(reader.rest, BY())
        }
      case _ =>
        reader.addAlternates(PARTITIONBYSET)
        ParseResult(reader, Nil)
    }
  }

  private def maybeAscDesc(reader: Reader): ParseResult[OrderDirection] = {
    reader.first match {
      case a@ASC() => ParseResult(reader.rest, a)
      case d@DESC() => ParseResult(reader.rest, d)
      case _ =>
        reader.addAlternates(ASCDESCSET)
        ParseResult(reader, ASC())
    }
  }

  private def maybeNullPlacement(reader: Reader): ParseResult[Option[NullPlacement]] = {
    reader.first match {
      case NULL() | NULLS() =>
        val direction =
          reader.rest.first match {
            case FIRST() => Some(First)
            case LAST() => Some(Last)
            case _ => fail(reader.rest, Set(FIRST(), LAST()))
          }
        ParseResult(reader.rest.rest, direction)
      case _ =>
        reader.addAlternates(NULLNULLSSET)
        ParseResult(reader, None)
    }
  }

  private def ordering(reader: Reader): ParseResult[OrderBy] = {
    val ParseResult(r2, criterion) = topLevelExpr(reader)
    val ParseResult(r3, ascDesc) = maybeAscDesc(r2)
    val ParseResult(r4, firstLast) = maybeNullPlacement(r3)

    ParseResult(r4, OrderBy(criterion, ascDesc == ASC(), firstLast.fold(ascDesc == ASC())(_ == Last)))
  }

  private def orderingList(reader: Reader): ParseResult[Seq[OrderBy]] = {
    val args = Vector.newBuilder[OrderBy]

    @tailrec
    def loop(reader: Reader): Reader = {
      val ParseResult(r2, candidate) = ordering(reader)
      args += candidate
      r2.first match {
        case COMMA() =>
          loop(r2.rest)
        case _ =>
          r2.addAlternates(Set[Expectation](COMMA()))
          r2
      }
    }
    val finalReader = loop(reader)

    ParseResult(finalReader, args.result())
  }

  private def windowOrderBy(reader: Reader): ParseResult[Seq[OrderBy]] = {
    reader.first match {
      case ORDER() =>
        reader.rest.first match {
          case BY() => orderingList(reader.rest.rest)
          case _ => fail1(reader.rest, BY())
        }
      case _ =>
        reader.addAlternates(ORDERBYSET)
        ParseResult(reader, Nil)
    }
  }

  private def tokenToLiteral(token: Token): Literal = {
    ast.StringLiteral(token.printable.toUpperCase)(token.position)
  }

  private def frameStartEnd(reader: Reader): ParseResult[Seq[Expression]] = {
    reader.first match {
      case a@UNBOUNDED() =>
        reader.rest.first match {
          case b@PRECEDING() =>
            ParseResult(reader.rest.rest, Seq(tokenToLiteral(a), tokenToLiteral(b)))
          case _ =>
            fail1(reader.rest, PRECEDING())
        }
      case a@CURRENT() =>
        reader.rest.first match {
          case b@ROW() =>
            ParseResult(reader.rest.rest, Seq(tokenToLiteral(a), tokenToLiteral(b)))
          case _ =>
            fail1(reader.rest, ROW())
        }
      case n: IntegerLiteral =>
        reader.rest.first match {
          case b@(PRECEDING() | FOLLOWING()) =>
            ParseResult(reader.rest.rest, Seq(ast.NumberLiteral(BigDecimal(n.asInt))(n.position), tokenToLiteral(b)))
          case _ =>
            fail(reader.rest, Set(PRECEDING(), FOLLOWING()))
        }
      case _ =>
        fail(reader, Set(UNBOUNDED(), CURRENT(), AnIntegerLiteral))
    }
  }

  private def windowFrameClause(reader: Reader): ParseResult[Seq[Expression]] = {
    reader.first match {
      case r@(RANGE() | ROWS()) =>
        reader.rest.first match {
          case b@BETWEEN() =>
            val ParseResult(r2, fa) = frameStartEnd(reader.rest.rest)
            r2.first match {
              case a@AND() =>
                frameStartEnd(r2.rest).map { fb =>
                  Seq(tokenToLiteral(r), tokenToLiteral(b)) ++ fa ++ Seq(tokenToLiteral(a)) ++ fb
                }
              case _ =>
                fail1(r2, AND())
            }
          case UNBOUNDED() | CURRENT() =>
            frameStartEnd(reader.rest).map { fb =>
              tokenToLiteral(r) +: fb
            }
          case _: IntegerLiteral =>
            frameStartEnd(reader.rest).map { fb =>
              tokenToLiteral(r) +: fb
            }
          case _ =>
            fail(reader.rest, Set(BETWEEN(), UNBOUNDED(), CURRENT(), AnIntegerLiteral))
        }
      case _ =>
        reader.addAlternates(RANGEROWSSET)
        ParseResult(reader, Nil)
    }
  }

  private def windowFunctionParams(reader: Reader): ParseResult[WindowFunctionInfo] = {
    reader.first match {
      case LPAREN() =>
        val ParseResult(r2, partition) = windowPartitionBy(reader.rest)
        val ParseResult(r3, orderBy) = windowOrderBy(r2)
        val ParseResult(r4, frame) = windowFrameClause(r3)
        r4.first match {
          case RPAREN() =>
            ParseResult(r4.rest, WindowFunctionInfo(partition, orderBy, frame))
          case _ =>
            fail1(r4, RPAREN())
        }
      case _ =>
        fail1(reader, LPAREN())
    }
  }

  private def parseFunctionOver(reader: Reader, fn: FunctionName, pos: Position, args: Seq[Expression]): ParseResult[Expression] = {
    reader.first match {
      case OVER() =>
        windowFunctionParams(reader.rest).map { wfp =>
          FunctionCall(fn, args, Some(wfp))(pos, pos)
        }
      case _ =>
        reader.addAlternates(OVERSET)
        ParseResult(reader, FunctionCall(fn, args, None)(pos, pos))
    }
  }

  private def identifierOrFuncall(reader: Reader, ident: Identifier): ParseResult[Expression] = {
    // we want to match
    //    id
    //    id(DISTINCT expr)
    //    id(*) [OVER windowstuff]
    //    id(expr,...) [OVER windowstuff]
    reader.first match {
      case LPAREN() =>
        reader.rest.first match {
          case DISTINCT() =>
            val ParseResult(r2, arg) = nestedExpr(reader.rest.rest)
            r2.first match {
              case RPAREN() =>
                // eww
                ParseResult(r2.rest, FunctionCall(FunctionName(ident.value.toLowerCase + "_distinct"), Seq(arg), None)(ident.pos, ident.pos))
              case _ =>
                fail1(reader.rest.rest, RPAREN())
            }
          case STAR() =>
            reader.rest.rest.first match {
              case RPAREN() =>
                parseFunctionOver(reader.rest.rest.rest, SpecialFunctions.StarFunc(ident.value), ident.position, Nil)
              case _ =>
                fail1(reader.rest.rest, RPAREN())
            }
          case RPAREN() =>
            parseFunctionOver(reader.rest.rest, FunctionName(ident.value), ident.position, Nil)
          case _ =>
            val ParseResult(r2, args) = parseArgList(reader.rest)
            parseFunctionOver(r2, FunctionName(ident.value), ident.position, args)
        }
      case _ =>
        reader.addAlternates(LPARENSET)
        ParseResult(reader, ColumnOrAliasRef(None, ColumnName(ident.value))(ident.position))
    }
  }

  private def intoQualifier(t: TableIdentifier): String =
    TableName.SodaFountainPrefix + t.value.substring(1) /* remove prefix @ */

  private def joinedColumn(reader: Reader, tableIdent: TableIdentifier): ParseResult[Expression] = {
    reader.first match {
      case DOT() =>
        reader.rest.first match {
          case si@SystemIdentifier(s, _) =>
            ParseResult(reader.rest.rest, ColumnOrAliasRef(Some(intoQualifier(tableIdent)), ColumnName(s))(tableIdent.position))
          case i@Identifier(s, _) =>
            ParseResult(reader.rest.rest, ColumnOrAliasRef(Some(intoQualifier(tableIdent)), ColumnName(s))(tableIdent.position))
          case _ =>
            fail1(reader.rest, AnIdentifier)
        }
      case _ =>
        fail1(reader, DOT())
    }
  }

  // Ok so after ALL THAT we're finally almost at the bottom!  This is
  // the thing I've arbitrarily decided is "atomic", on the grounds
  // that it is not self-recursive save by going all the way back up
  // to `expr`.  This is where it'd be nicest to be able to use the
  // parser-generator...
  @tailrec
  private def atom(reader: Reader, allowCase: Boolean = true): ParseResult[Expression] = {
    reader.first match {
      case c@CASE() if allowCase =>
        // just because we've seen a CASE doesn't mean we're in a case
        // expression; the user could be referring to a column called
        // "case" or using the case function.
        reader.rest.first match {
          case WHEN() =>
            // ok, this is enough to commit to it
            conditional(reader.rest, c.position)
          case _ =>
            atom(reader, allowCase = false)
        }
      case literal: LiteralToken =>
        val exprified =
          literal match {
            case n: tokens.NumberLiteral => ast.NumberLiteral(n.value)(n.position)
            case s: tokens.StringLiteral => ast.StringLiteral(s.value)(s.position)
            case b: tokens.BooleanLiteral => ast.BooleanLiteral(b.value)(b.position)
            case n: NULL => ast.NullLiteral()(n.position)
          }
        ParseResult(reader.rest, exprified)
      case open@LPAREN() =>
        val ParseResult(r2, e) = nestedExpr(reader.rest)
        r2.first match {
          case RPAREN() =>
            ParseResult(r2.rest, FunctionCall(SpecialFunctions.Parens, Seq(e), None)(open.position, open.position))
          case _ =>
            fail1(r2, RPAREN())
        }

      // and this is the place where we diverge the greatest from the
      // original parser.  This would accept all kinds of nonsense like
      // `@gfdsg.:gsdfg(DISTINCT bleh)` and then mangle that into
      // `gsdfg_distinct(bleh)`
      case ident: Identifier =>
        identifierOrFuncall(reader.rest, ident)
      case ident: SystemIdentifier =>
        ParseResult(reader.rest, ColumnOrAliasRef(None, ColumnName(ident.value))(ident.position))
      case table: TableIdentifier if allowJoins =>
        joinedColumn(reader.rest, table)

      case hole: HoleIdentifier if allowHoles =>
        ParseResult(reader.rest, Hole(HoleName(hole.value))(hole.position))

      case _ =>
        reader.resetAlternates()
        fail1(reader, AnExpression)
    }
  }

  private def dereference(reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, e) = atom(reader)
    dereference_!(r2, e)
  }

  @tailrec
  private def dereference_!(reader: Reader, arg: Expression): ParseResult[Expression] = {
    reader.first match {
      case dot@DOT() =>
        val ParseResult(r2, (ident, identPos)) = simpleIdentifier(reader.rest)
        dereference_!(r2, FunctionCall(SpecialFunctions.Subscript, Seq(arg, ast.StringLiteral(ident)(identPos)), None)(arg.position, dot.position))
      case open@LBRACKET() =>
        val ParseResult(r2, index) = nestedExpr(reader.rest)
        r2.first match {
          case RBRACKET() =>
            dereference_!(r2.rest, FunctionCall(SpecialFunctions.Subscript, Seq(arg, index), None)(arg.position, open.position))
          case _ =>
            r2.resetAlternates()
            fail1(r2, RBRACKET())
        }
      case _ =>
        reader.addAlternates(DOTLBRACKETSET)
        ParseResult(reader, arg)
    }
  }

  private def cast(reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, arg) = dereference(reader)
    cast_!(r2, arg)
  }

  @tailrec
  private def cast_!(reader: Reader, arg: Expression): ParseResult[Expression] = {
    reader.first match {
      case op@COLONCOLON() =>
        val ParseResult(r2, (ident, identPos)) = simpleIdentifier(reader.rest)
        cast_!(r2, FunctionCall(SpecialFunctions.Cast(TypeName(ident)), Seq(arg), None)(arg.position, identPos))
      case _ =>
        reader.addAlternates(OPERATORSET) // COLONCOLONSET - See note in order_!()
        ParseResult(reader, arg)
    }
  }

  private def unary(reader: Reader): ParseResult[Expression] = {
    reader.first match {
      case op@(MINUS() | PLUS()) =>
        unary(reader.rest).map { arg =>
          FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg), None)(op.position, op.position)
        }
      case _ =>
        reader.addAlternates(OPERATORSET) // MINUSPLUSSET - See note in order_!()
        cast(reader)
    }
  }

  private def exp(reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, arg) = unary(reader)
    r2.first match {
      case op@CARET() =>
        exp(r2.rest).map { arg2 =>
          FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg, arg2), None)(arg.position, op.position)
        }
      case _ =>
        reader.addAlternates(OPERATORSET) // CARETSET - See note in order_!()
        ParseResult(r2, arg)
    }
  }

  private def factor(reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, e) = exp(reader)
    factor_!(r2, e)
  }

  @tailrec
  private def factor_!(reader: Reader, arg: Expression): ParseResult[Expression] = {
    reader.first match {
      case op@(STAR() | SLASH() | PERCENT()) =>
        val ParseResult(r2, arg2) = exp(reader.rest)
        factor_!(r2, FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg, arg2), None)(arg.position, op.position))
      case _ =>
        reader.addAlternates(OPERATORSET) // FACTORSET - See note in order_!()
        ParseResult(reader, arg)
    }
  }

  private def term(reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, e) = factor(reader)
    term_!(r2, e)
  }

  @tailrec
  private def term_!(reader: Reader, arg: Expression): ParseResult[Expression] = {
    reader.first match {
      case op@(PLUS() | MINUS() | PIPEPIPE()) =>
        val ParseResult(r2, arg2) = factor(reader.rest)
        term_!(r2, FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg, arg2), None)(arg.position, op.position))
      case _ =>
        reader.addAlternates(OPERATORSET) // TERMSET - See note in order_!()
        ParseResult(reader, arg)
    }
  }

  private def order(reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, e) = term(reader)
    order_!(r2, e)
  }

  @tailrec
  private def order_!(reader: Reader, arg: Expression): ParseResult[Expression] = {
    reader.first match {
      case op@(EQUALS() | LESSGREATER() | LESSTHAN() | LESSTHANOREQUALS() | GREATERTHAN() | GREATERTHANOREQUALS() | EQUALSEQUALS() | BANGEQUALS()) =>
        val ParseResult(r2, arg2) = term(reader.rest)
        order_!(r2, FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg, arg2), None)(arg.position, op.position))
      case _ =>
        // So a bunch this adds an expectation of just "an operator"
        // rather than specifying the operators it expects, and the
        // functions above up through `cast` do not.  This only works
        // because these handful of functions are a unique chain of
        // parser functions - everything that enters enter order()
        // (and _only_ those things) will be able to pass through
        // those, and they pass through them without any intervening
        // parsing steps.  As a result, we can just collapse _all_ of
        // their expectations into just "we want some operator here".

        reader.addAlternates(OPERATORSET) // COMPARISONSET
        ParseResult(reader, arg)
    }
  }

  private def parseArgList(reader: Reader, arg0: Option[Expression] = None): ParseResult[Seq[Expression]] = {
    reader.first match {
      case RPAREN() =>
        ParseResult(reader.rest, Nil)
      case _ =>
        val args = Vector.newBuilder[Expression]
        args ++= arg0

        @tailrec
        def loop(reader: Reader): Reader = {
          val ParseResult(r2, candidate) = nestedExpr(reader)
          args += candidate
          r2.first match {
            case RPAREN() =>
              r2.rest
            case COMMA() =>
              loop(r2.rest)
            case _ =>
              fail(r2, Set(RPAREN(), COMMA()))
          }
        }

        val finalReader = loop(reader)

        ParseResult(finalReader, args.result())
    }
  }

  private def parseIn(name: FunctionName, scrutinee: Expression, op: Token, reader: Reader): ParseResult[Expression] = {
    reader.first match {
      case LPAREN() =>
        parseArgList(reader.rest, arg0 = Some(scrutinee)).map { args =>
          FunctionCall(name, args, None)(scrutinee.position, op.position)
        }
      case _ =>
        fail1(reader, LPAREN())
    }
  }

  private def parseLike(name: FunctionName, scrutinee: Expression, op: Token, reader: Reader): ParseResult[Expression] = {
    likeBetweenIn(reader).map { pattern =>
      FunctionCall(name, Seq(scrutinee, pattern), None)(scrutinee.position, op.position)
    }
  }

  private def parseBetween(name: FunctionName, scrutinee: Expression, op: Token, reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, lowerBound) = likeBetweenIn(reader)
    r2.first match {
      case AND() =>
        likeBetweenIn(r2.rest).map { upperBound =>
          FunctionCall(name, Seq(scrutinee, lowerBound, upperBound), None)(scrutinee.position, op.position)
        }
      case _ =>
        r2.resetAlternates()
        fail1(r2, AND())
    }
  }

  // This is an annoying bit of grammar.  This node is defined as
  //   likeBetweenIn [bunchofcases] | order
  // so we need to rewrite it to eliminate the left-recursion.
  // Again, it becomes
  //    lBI = order lBI'
  //    lBI' = [the various cases] lBI' | ε
  private def likeBetweenIn(reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, arg) = order(reader)
    likeBetweenIn_!(r2, arg)
  }

  @tailrec
  private def likeBetweenIn_!(reader: Reader, arg: Expression): ParseResult[Expression] = {
    reader.first match {
      case is: IS =>
        // This is either IS NULL or IS NOT NULL
        val ParseResult(r2, arg2) =
          reader.rest.first match {
            case NULL() =>
              ParseResult(reader.rest.rest, FunctionCall(SpecialFunctions.IsNull, Seq(arg), None)(arg.position, is.position))
            case NOT() =>
              reader.rest.rest.first match {
                case NULL() =>
                  ParseResult(reader.rest.rest.rest, FunctionCall(SpecialFunctions.IsNotNull, Seq(arg), None)(arg.position, is.position))
                case _ =>
                  fail1(reader.rest.rest, NULL())
              }
            case _ =>
              fail(reader.rest, Set(NULL(), NOT()))
          }
        likeBetweenIn_!(r2, arg2)
      case like: LIKE =>
        // woo only one possibility!
        val ParseResult(r2, arg2) = parseLike(SpecialFunctions.Like, arg, like, reader.rest)
        likeBetweenIn_!(r2, arg2)
      case between: BETWEEN =>
        val ParseResult(r2, arg2) = parseBetween(SpecialFunctions.Between, arg, between, reader.rest)
        likeBetweenIn_!(r2, arg2)
      case not: NOT =>
        // could be NOT BETWEEN, NOT IN, or NOT LIKE
        reader.rest.first match {
          case between: BETWEEN =>
            val ParseResult(r2, arg2) = parseBetween(SpecialFunctions.NotBetween, arg, not, reader.rest.rest)
            likeBetweenIn_!(r2, arg2)
          case in: IN =>
            val ParseResult(r2, arg2) = parseIn(SpecialFunctions.NotIn, arg, not, reader.rest.rest)
            likeBetweenIn_!(r2, arg2)
          case like: LIKE =>
            val ParseResult(r2, arg2) = parseLike(SpecialFunctions.NotLike, arg, not, reader.rest.rest)
            likeBetweenIn_!(r2, arg2)
          case other =>
            fail(reader.rest, Set(BETWEEN(), IN(), LIKE()))
        }
      case in: IN =>
        // Again only one possibility!
        val ParseResult(r2, arg2) = parseIn(SpecialFunctions.In, arg, in, reader.rest)
        likeBetweenIn_!(r2, arg2)
      case _ =>
        reader.addAlternates(LIKEBETWEENINSET)
        ParseResult(reader, arg)
    }
  }

  private def negation(reader: Reader): ParseResult[Expression] = {
    reader.first match {
      case op: NOT =>
        negation(reader.rest).map { arg =>
          FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg), None)(op.position, op.position)
        }
      case _ =>
        reader.addAlternates(NOTSET)
        likeBetweenIn(reader)
    }
  }

  private def conjunction(reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, e) = negation(reader)
    conjunction_!(r2, e)
  }

  @tailrec
  private def conjunction_!(reader: Reader, arg: Expression): ParseResult[Expression] = {
    reader.first match {
      case and: AND =>
        val ParseResult(r2, arg2) = negation(reader.rest)
        conjunction_!(r2, FunctionCall(SpecialFunctions.Operator(and.printable), Seq(arg, arg2), None)(arg.position, and.pos))
      case _ =>
        reader.addAlternates(ANDSET)
        ParseResult(reader, arg)
    }
  }

  // This is a pattern that'll be used all over this parser.  The
  // combinators approach was
  //    disjunction = (disjunction OR conjunction) | conjunction;
  // which is left-recursive.  We can eliminate that by instead doing
  //    disjunction = conjunction disjunction'
  //    disjunction' = OR conjunction disjunction' | ε
  private def disjunction(reader: Reader): ParseResult[Expression] = {
    val ParseResult(r2, e) = conjunction(reader)
    disjunction_!(r2, e)
  }

  @tailrec
  private def disjunction_!(reader: Reader, arg: Expression): ParseResult[Expression] = {
    reader.first match {
      case or: OR =>
        val ParseResult(r2, arg2) = conjunction(reader.rest)
        disjunction_!(r2, FunctionCall(SpecialFunctions.Operator(or.printable), Seq(arg, arg2), None)(arg.position, or.pos))
      case _ =>
        reader.addAlternates(ORSET)
        ParseResult(reader, arg)
    }
  }

  private def nestedExpr(reader: Reader): ParseResult[Expression] = {
    disjunction(reader)
  }

  private def topLevelExpr(reader: Reader): ParseResult[Expression] = {
    val r = disjunction(reader)
    r.reader.resetAlternates()
    r
  }
}
