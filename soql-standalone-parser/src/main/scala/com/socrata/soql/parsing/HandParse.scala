package com.socrata.soql.parsing

import scala.util.parsing.input.Position
import scala.annotation.tailrec

import com.socrata.NonEmptySeq
import com.socrata.soql.ast._
import com.socrata.soql.tokens._
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery, ast, tokens}
import com.socrata.soql.environment.{ColumnName, FunctionName, HoleName, TableName, TypeName}

object HandRolledParser {
  private class Keyword(s: String) {
    def unapply(ident: Identifier): Boolean = {
      !ident.quoted && ident.value.equalsIgnoreCase(s)
    }
    def identifier = Identifier(s, false)
  }

  sealed abstract class Tokenlike {
    def printable: String
  }
  case class ActualToken(token: Token) extends Tokenlike {
    def printable = token.printable
  }
  case object AnIntegerLiteral extends Tokenlike {
    def printable = "an integer"
  }
  case object AStringLiteral extends Tokenlike {
    def printable = "a string"
  }
  case object ASystemIdentifier extends Tokenlike {
    def printable = "a system identifier"
  }
  case object AUserIdentifier extends Tokenlike {
    def printable = "a user identifier"
  }
  case object ATableIdentifier extends Tokenlike {
    def printable = "a table identifier"
  }

  private implicit def tokenAsTokenLike(t: Token): Tokenlike = ActualToken(t)

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

  trait ParseException extends Exception {
    def position: Position = reader.first.position
    val reader: Reader
  }

  private type Reader = scala.util.parsing.input.Reader[Token]
}

abstract class HandRolledParser(parameters: AbstractParser.Parameters = AbstractParser.defaultParameters) {
  import parameters._
  import HandRolledParser._

  def binaryTreeSelect(soql: String): BinaryTree[Select] = parseFull(compoundSelect, soql)

  def selection(soql: String): Selection = ???
  def joins(soql: String): Seq[Join] = parseFull(joinList, soql)
  def expression(soql: String): Expression = parseFull(expr, soql)
  def orderings(soql: String): Seq[OrderBy] = parseFull(orderingList, soql)
  def groupBys(soql: String): Seq[Expression] = parseFull(commaSeparatedExprs, soql)

  def selectStatement(soql: String): NonEmptySeq[Select] = parseFull(pipedSelect, soql)
  def unchainedSelectStatement(soql: String): Select = parseFull(select, soql)
  def parseJoinSelect(soql: String): JoinSelect = parseFull(joinSelect, soql)
  def limit(soql: String): BigInt = ???
  def offset(soql: String): BigInt = ???
  def search(soql: String): String = ???

  protected def lexer(s: String): AbstractLexer
  protected def expectedEOF(reader: Reader): ParseException
  protected def expectedTokens(reader: Reader, token: Set[Tokenlike]): ParseException
  protected def expectedToken(reader: Reader, token: Tokenlike): ParseException = expectedTokens(reader, Set(token))
  protected def expectedExpression(reader: Reader): ParseException
  protected def expectedIdentifier(reader: Reader): ParseException

  private def parseFull[T](parser: Reader => (Reader, T), soql: String): T = {
    val (end, result) = parser(new LexerReader(lexer(soql)))
    if(end.first.isInstanceOf[EOF]) {
      result
    } else {
      throw expectedEOF(end)
    }
  }

  private def tokenParser[T <: Token](token: T)(reader: Reader): Option[(Reader, Token)] = {
    if(reader.first == token) {
      Some((reader.rest, reader.first))
    } else {
      None
    }
  }

  private def select(reader: Reader): (Reader, Select) = {
    reader.first match {
      case SELECT() =>
        val (r2, d) = distinct(reader.rest)
        val (r3, selected) = selectList(r2)
        val (r4, fromClause) = from(r3)
        val (r5, joinClause) = if(allowJoins) joinList(r3) else (r4, Seq.empty)
        val (r6, whereClause) = where(r5)
        val (r7, groupByClause) = groupBy(r6)
        val (r8, havingClause) = having(r7)
        val (r9, (orderByClause, searchClause)) = orderByAndSearch(r8)
        val (r10, (limitClause, offsetClause)) = limitOffset(r9)
        (r10, Select(d, selected, fromClause, joinClause, whereClause, groupByClause, havingClause, orderByClause, limitClause, offsetClause, searchClause))
      case _ =>
        throw expectedToken(reader, SELECT())
    }
  }

  private def limitOffset(reader: Reader): (Reader, (Option[BigInt], Option[BigInt])) = {
    reader.first match {
      case LIMIT() =>
        val (r2, limitClause) = limit(reader)
        val (r3, offsetClause) = offset(r2)
        (r3, (limitClause, offsetClause))
      case OFFSET() =>
        val (r2, offsetClause) = offset(reader)
        val (r3, limitClause) = limit(r2)
        (r3, (limitClause, offsetClause))
      case _ =>
        (reader, (None, None))
    }
  }

  private def limit(reader: Reader): (Reader, Option[BigInt]) = {
    reader.first match {
      case LIMIT() =>
        reader.rest.first match {
          case lit: IntegerLiteral => (reader.rest.rest, Some(lit.asInt))
          case _ => throw expectedToken(reader.rest, AnIntegerLiteral)
        }
      case _ =>
        (reader, None)
    }
  }


  private def offset(reader: Reader): (Reader, Option[BigInt]) = {
    reader.first match {
      case OFFSET() =>
        reader.rest.first match {
          case lit: IntegerLiteral => (reader.rest.rest, Some(lit.asInt))
          case _ => throw expectedToken(reader.rest, AnIntegerLiteral)
        }
      case _ =>
        (reader, None)
    }
  }

  private def orderByAndSearch(reader: Reader): (Reader, (Seq[OrderBy], Option[String])) = {
    reader.first match {
      case ORDER() =>
        val (r2, orderByClause) = orderBy(reader)
        val (r3, searchClause) = search(r2)
        (r3, (orderByClause, searchClause))
      case SEARCH() =>
        val (r2, searchClause) = search(reader)
        val (r3, orderByClause) = orderBy(r2)
        (r3, (orderByClause, searchClause))
      case _ =>
        (reader, (Nil, None))
    }
  }

  private def orderBy(reader: Reader): (Reader, Seq[OrderBy]) = {
    reader.first match {
      case ORDER() =>
        reader.rest.first match {
          case BY() =>
            orderingList(reader.rest.rest)
          case _ =>
            throw expectedToken(reader.rest, BY.identifier)
        }
      case _ =>
        (reader, Nil)
    }
  }

  private def search(reader: Reader): (Reader, Option[String]) = {
    reader.first match {
      case SEARCH() =>
        reader.rest.first match {
          case tokens.StringLiteral(s) =>
            (reader.rest.rest, Some(s))
          case _ =>
            throw expectedToken(reader.rest, AStringLiteral)
        }
      case _ =>
        (reader, None)
    }
  }

  private def groupBy(reader: Reader): (Reader, Seq[Expression]) = {
    reader.first match {
      case GROUP() =>
        reader.rest.first match {
          case BY() =>
            commaSeparatedExprs(reader.rest.rest)
          case _ =>
            throw expectedToken(reader, BY.identifier)
        }
      case _ =>
        (reader, Nil)
    }
  }

  private def where(reader: Reader): (Reader, Option[Expression]) = {
    reader.first match {
      case WHERE() =>
        val (r2, e) = expr(reader.rest)
        (r2, Some(e))
      case _ =>
        (reader, None)
    }
  }

  private def having(reader: Reader): (Reader, Option[Expression]) = {
    reader.first match {
      case HAVING() =>
        val (r2, e) = expr(reader.rest)
        (r2, Some(e))
      case _ =>
        (reader, None)
    }
  }

  private def selectList(reader: Reader): (Reader, Selection) = {
    val (r2, systemStar) = systemStarSelection(reader)
    val (r3, userStars) = userStarSelections(r2, systemStar.isDefined)
    val (r4, expressions) = expressionSelectList(r3, systemStar.isDefined || userStars.nonEmpty)
    (r4, Selection(systemStar, userStars, expressions))
  }

  private def systemStarSelection(reader: Reader): (Reader, Option[StarSelection]) = {
    reader.first match {
      case ti: TableIdentifier =>
        reader.rest.first match {
          case DOT() =>
            reader.rest.rest.first match {
              case star@COLONSTAR() =>
                val (r2, exceptions) = selectExceptions(reader.rest.rest.rest, systemIdentifier)
                (r2, Some(StarSelection(Some(intoQualifier(ti)), exceptions).positionedAt(star.position)))
              case _ =>
                // whoops, we're not parsing a :* at all, bail out
                (reader, None)
            }
          case _ =>
            throw expectedToken(reader.rest, DOT())
        }
      case star@COLONSTAR() =>
        val (r2, exceptions) = selectExceptions(reader.rest, systemIdentifier)
        (r2, Some(StarSelection(None, exceptions).positionedAt(star.position)))
      case _ =>
        (reader, None)
    }
  }

  private def selectExceptions(reader: Reader, identParser: Reader => (Reader, (ColumnName, Position))): (Reader, Seq[(ColumnName, Position)]) = {
    reader.first match {
      case LPAREN() =>
        reader.rest.first match {
          case EXCEPT() =>
            val result = Vector.newBuilder[(ColumnName, Position)]
            @tailrec
            def loop(reader: Reader): Reader = {
              val (r2, ident) = identParser(reader)
              result += ident
              r2.first match {
                case COMMA() =>
                  loop(r2.rest)
                case RPAREN() =>
                  r2.rest
                case _ =>
                  throw expectedTokens(r2, Set(COMMA(), RPAREN()))
              }
            }
            val finalReader = loop(reader.rest.rest)
            (finalReader, result.result())
          case _ =>
            throw expectedToken(reader.rest, EXCEPT())
        }
      case _ =>
        (reader, Nil)
    }
  }

  private def systemIdentifier(reader: Reader): (Reader, (ColumnName, Position)) = {
    reader.first match {
      case si: SystemIdentifier =>
        (reader.rest, (ColumnName(si.value), si.position))
      case _ =>
        throw expectedToken(reader, ASystemIdentifier)
    }
  }

  private def userIdentifier(reader: Reader): (Reader, (ColumnName, Position)) = {
    reader.first match {
      case si: Identifier =>
        (reader.rest, (ColumnName(si.value), si.position))
      case _ =>
        throw expectedToken(reader, AUserIdentifier)
    }
  }

  private def userStarSelections(reader: Reader, commaFirst: Boolean): (Reader, Seq[StarSelection]) = {
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
                  val (r2, exceptions) = selectExceptions(r.rest.rest.rest, userIdentifier)
                  result += StarSelection(Some(intoQualifier(ti)), exceptions).positionedAt(star.position)
                  loop(r2, commaFirst = true)
                case _ =>
                  // Whoops, not a star selection, revert
                  reader
              }
            case _ =>
              reader
          }
        case star@STAR() =>
          val (r2, exceptions) = selectExceptions(r.rest, userIdentifier)
          result += StarSelection(None, exceptions).positionedAt(star.position)
          loop(r2, commaFirst = true)
        case _ =>
          reader
      }
    }

    val finalReader = loop(reader, commaFirst)
    (finalReader, result.result())
  }

  private def expressionSelectList(reader: Reader, commaFirst: Boolean): (Reader, Seq[SelectedExpression]) = {
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
          Some(expr(r))
        } catch {
          case e: ParseException if !commaFirst && e.reader == r =>
            // ok, so if the parse failed right at the start we'll
            // assume we're good at the end of the select list and are
            // good to move on...
            None
        }

      exprResult match {
        case Some((r2, e)) =>
          r2.first match {
            case AS() =>
              // now we must see an identifier...
              val (r3, identPos) = userIdentifier(r2.rest)
              result += SelectedExpression(e, Some(identPos))
              loop(r3, commaFirst = true)
            case _ =>
              result += SelectedExpression(e, None)
              loop(r2, commaFirst = true)
          }
        case None =>
          reader
      }
    }

    val finalReader = loop(reader, commaFirst)
    (finalReader, result.result())
  }

  private def joinList(reader: Reader): (Reader, Seq[Join]) = {
    val acc = Vector.newBuilder[Join]

    @tailrec
    def loop(reader: Reader): Reader = {
      val (r2, joinClause) = join(reader)
      joinClause match {
        case Some(j) =>
          acc += j
          loop(r2)
        case None =>
          r2
      }
    }

    val finalReader = loop(reader)
    (finalReader, acc.result())
  }

  private def join(reader: Reader): (Reader, Option[Join]) = {
    reader.first match {
      case direction@(LEFT() | RIGHT() | FULL()) =>
        val r2 = reader.rest.first match {
          case OUTER() => reader.rest.rest
          case _ => reader.rest
        }
        r2.first match {
          case JOIN() =>
            val (r3, j) = finishJoin(r2.rest, OuterJoin(direction, _, _, _))
            (r3, Some(j))
          case _ =>
            throw expectedToken(reader, JOIN())
        }
      case JOIN() =>
        val (r2, j) = finishJoin(reader.rest, InnerJoin(_, _, _))
        (r2, Some(j))
      case _ =>
        (reader, None)
    }
  }

  private def finishJoin(reader: Reader, join: (JoinSelect, Expression, Boolean) => Join): (Reader, Join) = {
    val (r2, isLateral) = reader.first match {
      case LATERAL() => (reader.rest, true)
      case _ => (reader, false)
    }
    val (r3, select) = joinSelect(r2)
    r3.first match {
      case ON() =>
        val (r4, e) = expr(r3.rest)
        (r4, join(select, e, isLateral))
      case _ =>
        throw expectedToken(r3, ON())
    }
  }

  private def joinSelect(reader: Reader): (Reader, JoinSelect) = {
    reader.first match {
      case ti: TableIdentifier =>
        // could be JoinTable, could be JoinFunc if that's allowed here
        reader.rest.first match {
          case AS() =>
            // JoinTable
            val (r2, a) = tableAlias(reader.rest.rest)
            (r2, JoinTable(TableName(intoQualifier(ti), Some(a))))
          case LPAREN() if allowJoinFunctions =>
            // JoinFunc
            val (r2, args) = parseArgList(reader.rest.rest)
            r2.first match {
              case AS() =>
                val (r3, a) = tableAlias(r2.rest)
                (r3, JoinFunc(TableName(intoQualifier(ti), Some(a)), args)(ti.position))
              case _ =>
                (r2, JoinFunc(TableName(intoQualifier(ti), None), args)(ti.position))
            }
          case _ =>
            // Also JoinTable
            (reader.rest, JoinTable(TableName(intoQualifier(ti), None)))
        }
      case _ =>
        val (r2, s) = atomSelect(reader)
        r2.first match {
          case AS() =>
            val (r3, alias) = tableAlias(r2.rest)
            (r3, JoinQuery(s, alias))
          case _ =>
            throw expectedToken(r2, AS())
        }
    }
  }

  private def atomSelect(reader: Reader): (Reader, BinaryTree[Select]) = {
    reader.first match {
      case LPAREN() =>
        parenSelect(reader.rest)
      case _ =>
        val (r2, s) = select(reader)
        (r2, Leaf(s))
    }
  }

  // already past the lparen
  private def parenSelect(reader: Reader): (Reader, BinaryTree[Select]) = {
    val (r2, s) = compoundSelect(reader)
    r2.first match {
      case RPAREN() =>
        (r2.rest, s)
      case _ =>
        throw expectedToken(r2, RPAREN())
    }
  }

  private def pipedSelect(reader: Reader): (Reader, NonEmptySeq[Select]) = {
    val (r2, s1) = select(reader)
    val tail = Vector.newBuilder[Select]

    @tailrec
    def loop(reader: Reader): Reader = {
      reader.first match {
        case QUERYPIPE() =>
          val (r2, s) = select(reader.rest)
          tail += s
          loop(r2)
        case _ =>
          reader
      }
    }
    val finalReader = loop(r2)

    (finalReader, NonEmptySeq(s1, tail.result()))
  }

  private def compoundSelect(reader: Reader): (Reader, BinaryTree[Select]) = {
    val (r2, s) = atomSelect(reader)
    compoundSelect_!(r2, s)
  }

  private def compoundSelect_!(reader: Reader, arg: BinaryTree[Select]): (Reader, BinaryTree[Select]) = {
    reader.first match {
      case QUERYPIPE() =>
        val (r2, arg2) = atomSelect(reader.rest)
        arg2.asLeaf match {
          case Some(leaf) =>
            (r2, PipeQuery(arg, Leaf(leaf)))
          case None =>
            ??? // TODO: leaf query on the right expected
        }
      case op@(QUERYUNION() | QUERYINTERSECT() | QUERYMINUS() | QUERYUNIONALL() | QUERYINTERSECTALL() | QUERYMINUSALL()) =>
        val (r2, arg2) = atomSelect(reader.rest)
        compoundSelect_!(r2, Compound(op.printable, arg, arg2))
      case _ =>
        (reader, arg)
    }
  }

  private def distinct(reader: Reader): (Reader, Boolean) = {
    reader.first match {
      case DISTINCT() =>
        (reader.rest, true)
      case _ =>
        (reader, false)
    }
  }

  private def from(reader: Reader): (Reader, Option[TableName]) = {
    reader.first match {
      case FROM() =>
        // sadness: this loses position information
        val tn =
          reader.rest.first match {
            case tn: TableIdentifier => intoQualifier(tn)
            case _ => throw expectedToken(reader.rest, ATableIdentifier)
          }
        val (r2, alias) =
          reader.rest.rest.first match {
            case AS() =>
              val (r3, ta) = tableAlias(reader.rest.rest.rest)
              (r3, Some(ta))
            case _ =>
              (reader.rest.rest, None)
          }
        val tableName = TableName(tn, alias)
        if(tableName.nameWithSodaFountainPrefix == TableName.This && alias.isEmpty) {
          ??? // TODO: @this must have alias
        }
        (r2, Some(tableName))
      case _ =>
        (reader, None)
    }
  }

  private def tableAlias(reader: Reader): (Reader, String) = {
    val ta =
      reader.first match {
        case ident: Identifier => ident.value
        case ident: TableIdentifier => intoQualifier(ident)
        case _ => throw expectedTokens(reader, Set(AUserIdentifier, ATableIdentifier))
      }
    (reader.rest, TableName.withSodaFountainPrefix(ta))
  }

  private def simpleIdentifier(reader: Reader): (Reader, (String, Position)) =
    reader.first match {
      case i: Identifier => (reader.rest, (i.value, i.position))
      case _ => throw expectedIdentifier(reader)
    }

  private def conditional(reader: Reader, casePos: Position): (Reader, Expression) = {
    // we're given a reader that's already past the CASE, so we just
    // need to parse (WHEN expr THEN expr)+ ELSE expr END
    val cases = Vector.newBuilder[Expression]

    @tailrec
    def loop(reader: Reader): Either[Reader, Reader] = {
      // this is in a reader that has consumed the WHEN and therefore is reading a condition
      val (r2, condition) = expr(reader)
      r2.first match {
        case THEN() =>
          val (r3, consequent) = expr(r2.rest)
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
              throw expectedTokens(r3, Set(WHEN.identifier, ELSE.identifier, END.identifier))
          }
        case _ =>
          throw expectedToken(r2, THEN.identifier)
      }
    }

    reader.first match {
      case WHEN() =>
        val finalReader =
          loop(reader.rest) match {
            case Left(elseReader) =>
              val (r2, alternate) = expr(elseReader)
              cases += ast.BooleanLiteral(true)(casePos)
              cases += alternate
              r2.first match {
                case END() => r2.rest
                case _ => throw expectedToken(r2, END.identifier)
              }
            case Right(endReader) =>
              cases += ast.BooleanLiteral(false)(casePos)
              cases += ast.NullLiteral()(casePos)
              endReader
          }
        (finalReader, FunctionCall(SpecialFunctions.Case, cases.result(), None)(casePos, casePos))
      case _ =>
        throw expectedToken(reader, WHEN.identifier)
    }
  }

  private def commaSeparatedExprs(reader: Reader): (Reader, Seq[Expression]) = {
    val args = Vector.newBuilder[Expression]

    @tailrec
    def loop(reader: Reader): Reader = {
      val (r2, candidate) = expr(reader)
      args += candidate
      r2.first match {
        case COMMA() =>
          loop(r2.rest)
        case _ =>
          r2
      }
    }
    val finalReader = loop(reader)

    (finalReader, args.result())
  }

  private def windowPartitionBy(reader: Reader): (Reader, Seq[Expression]) = {
    reader.first match {
      case PARTITION() =>
        reader.rest.first match {
          case BY() => commaSeparatedExprs(reader.rest.rest)
          case _ => throw expectedToken(reader.rest, BY.identifier)
        }
      case _ =>
        (reader, Nil)
    }
  }

  private def maybeAscDesc(reader: Reader): (Reader, OrderDirection) = {
    reader.first match {
      case a@ASC() => (reader.rest, a)
      case d@DESC() => (reader.rest, d)
      case _ => (reader, ASC())
    }
  }

  private def maybeNullPlacement(reader: Reader): (Reader, Option[NullPlacement]) = {
    reader.first match {
      case NULL() | NULLS() =>
        val direction =
          reader.rest.first match {
            case FIRST() => Some(First)
            case LAST() => Some(Last)
            case _ => throw expectedTokens(reader.rest, Set(FIRST.identifier, LAST.identifier))
          }
        (reader.rest.rest, direction)
      case _ =>
        (reader, None)
    }
  }

  private def ordering(reader: Reader): (Reader, OrderBy) = {
    val (r2, criterion) = expr(reader)
    val (r3, ascDesc) = maybeAscDesc(r2)
    val (r4, firstLast) = maybeNullPlacement(r3)

    (r4, OrderBy(criterion, ascDesc == ASC(), firstLast.fold(ascDesc == ASC())(_ == Last)))
  }

  private def orderingList(reader: Reader): (Reader, Seq[OrderBy]) = {
    val args = Vector.newBuilder[OrderBy]

    @tailrec
    def loop(reader: Reader): Reader = {
      val (r2, candidate) = ordering(reader)
      args += candidate
      r2.first match {
        case COMMA() =>
          loop(r2.rest)
        case _ =>
          r2
      }
    }
    val finalReader = loop(reader)

    (finalReader, args.result())
  }

  private def windowOrderBy(reader: Reader): (Reader, Seq[OrderBy]) = {
    reader.first match {
      case ORDER() =>
        reader.rest.first match {
          case BY() => orderingList(reader.rest.rest)
          case _ => throw expectedToken(reader.rest, BY.identifier)
        }
      case _ =>
        (reader, Nil)
    }
  }

  private def tokenToLiteral(token: Token): Literal = {
    ast.StringLiteral(token.printable)(token.position)
  }

  private def frameStartEnd(reader: Reader): (Reader, Seq[Expression]) = {
    reader.first match {
      case a@UNBOUNDED() =>
        reader.rest.first match {
          case b@PRECEDING() =>
            (reader.rest.rest, Seq(tokenToLiteral(a), tokenToLiteral(b)))
          case _ =>
            throw expectedToken(reader.rest, PRECEDING.identifier)
        }
      case a@CURRENT() =>
        reader.rest.first match {
          case b@ROW() =>
            (reader.rest.rest, Seq(tokenToLiteral(a), tokenToLiteral(b)))
          case _ =>
            throw expectedToken(reader.rest, ROW.identifier)
        }
      case n: IntegerLiteral =>
        reader.rest.first match {
          case b@(PRECEDING() | FOLLOWING()) =>
            (reader.rest.rest, Seq(ast.NumberLiteral(BigDecimal(n.asInt))(n.position), tokenToLiteral(b)))
          case _ =>
            throw expectedTokens(reader.rest, Set(PRECEDING.identifier, FOLLOWING.identifier))
        }
      case _ =>
        throw expectedTokens(reader, Set(UNBOUNDED.identifier, CURRENT.identifier, AnIntegerLiteral))
    }
  }

  private def windowFrameClause(reader: Reader): (Reader, Seq[Expression]) = {
    reader.first match {
      case r@(RANGE() | ROWS()) =>
        reader.rest.first match {
          case b@BETWEEN() =>
            val (r2, fa) = frameStartEnd(reader.rest.rest)
            r2.first match {
              case a@AND() =>
                val (r3, fb) = frameStartEnd(r2.rest)
                (r3, Seq(tokenToLiteral(r), tokenToLiteral(b)) ++ fa ++ Seq(tokenToLiteral(a)) ++ fb)
              case _ =>
                throw expectedToken(r2, AND())
            }
          case UNBOUNDED() =>
            frameStartEnd(reader.rest)
          case CURRENT() =>
            frameStartEnd(reader.rest)
          case _: IntegerLiteral =>
            frameStartEnd(reader.rest)
          case _ =>
            throw expectedTokens(reader.rest, Set(BETWEEN(), UNBOUNDED.identifier, CURRENT.identifier, AnIntegerLiteral))
        }
      case _ =>
        (reader, Nil)
    }
  }

  private def windowFunctionParams(reader: Reader): (Reader, WindowFunctionInfo) = {
    reader.first match {
      case LPAREN() =>
        val (r2, partition) = windowPartitionBy(reader.rest)
        val (r3, orderBy) = windowOrderBy(r2)
        val (r4, frame) = windowFrameClause(r3)
        r4.first match {
          case RPAREN() =>
            (r4.rest, WindowFunctionInfo(partition, orderBy, frame))
          case _ =>
            throw expectedToken(reader, RPAREN())
        }
      case _ =>
        throw expectedToken(reader, LPAREN())
    }
  }

  private def parseFunctionOver(reader: Reader, fn: FunctionName, pos: Position, args: Seq[Expression]): (Reader, Expression) = {
    reader.first match {
      case OVER() =>
        val (r2, wfp) = windowFunctionParams(reader.rest)
        (r2, FunctionCall(fn, args, Some(wfp))(pos, pos))
      case _ =>
        (reader, FunctionCall(fn, args, None)(pos, pos))
    }
  }

  private def identifierOrFuncall(reader: Reader, ident: Identifier): (Reader, Expression) = {
    // we want to match
    //    id
    //    id(DISTINCT expr)
    //    id(*) [OVER windowstuff]
    //    id(expr,...) [OVER windowstuff]
    reader.first match {
      case LPAREN() =>
        reader.rest.first match {
          case DISTINCT() =>
            val (r2, arg) = expr(reader.rest.rest)
            r2.first match {
              case RPAREN() =>
                // eww
                (r2.rest, FunctionCall(FunctionName(ident.value.toLowerCase + "_distinct"), Seq(arg), None)(ident.pos, ident.pos))
              case _ =>
                throw expectedToken(reader.rest.rest, RPAREN())
            }
          case STAR() =>
            reader.rest.rest.first match {
              case RPAREN() =>
                parseFunctionOver(reader.rest.rest.rest, SpecialFunctions.StarFunc(ident.value), ident.position, Nil)
              case _ =>
                throw expectedToken(reader.rest.rest, RPAREN())
            }
          case RPAREN() =>
            parseFunctionOver(reader.rest.rest, FunctionName(ident.value), ident.position, Nil)
          case _ =>
            val (r2, args) = parseArgList(reader.rest)
            parseFunctionOver(r2, FunctionName(ident.value), ident.position, args)
        }
      case _ =>
        (reader, ColumnOrAliasRef(None, ColumnName(ident.value))(ident.position))
    }
  }

  private def intoQualifier(t: TableIdentifier): String =
    TableName.SodaFountainPrefix + t.value.substring(1) /* remove prefix @ */

  private def joinedColumn(reader: Reader, tableIdent: TableIdentifier): (Reader, Expression) = {
    reader.first match {
      case DOT() =>
        reader.rest.first match {
          case si@SystemIdentifier(s, _) =>
            (reader.rest.rest, ColumnOrAliasRef(Some(intoQualifier(tableIdent)), ColumnName(s))(tableIdent.position))
          case i@Identifier(s, _) =>
            (reader.rest.rest, ColumnOrAliasRef(Some(intoQualifier(tableIdent)), ColumnName(s))(tableIdent.position))
          case _ =>
            throw expectedIdentifier(reader.rest)
        }
      case _ =>
        throw expectedToken(reader, DOT())
    }
  }

  // Ok so after ALL THAT we're finally almost at the bottom!  This is
  // the thing I've arbitrarily decided is "atomic", on the grounds
  // that it is not self-recursive save by going all the way back up
  // to `expr`.  This is where it'd be nicest to be able to use the
  // parser-generator...
  @tailrec
  private def atom(reader: Reader, allowCase: Boolean = true): (Reader, Expression) = {
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
        (reader.rest, exprified)
      case open@LPAREN() =>
        val (r2, e) = expr(reader.rest)
        r2.first match {
          case RPAREN() =>
            (r2.rest, FunctionCall(SpecialFunctions.Parens, Seq(e), None)(open.position, open.position))
          case _ =>
            throw expectedToken(r2, RPAREN())
        }

      // and this is the place where we diverge the greatest from the
      // original parser.  This would accept all kinds of nonsense like
      // `@gfdsg.:gsdfg(DISTINCT bleh)` and then mangle that into
      // `gsdfg_distinct(bleh)`
      case ident: Identifier =>
        identifierOrFuncall(reader.rest, ident)
      case ident: SystemIdentifier =>
        (reader.rest, ColumnOrAliasRef(None, ColumnName(ident.value))(ident.position))
      case table: TableIdentifier if allowJoins =>
        joinedColumn(reader.rest, table)

      case _ =>
        throw expectedExpression(reader)
    }
  }

  private def dereference(reader: Reader): (Reader, Expression) = {
    val (r2, e) = atom(reader)
    dereference_!(r2, e)
  }

  @tailrec
  private def dereference_!(reader: Reader, arg: Expression): (Reader, Expression) = {
    reader.first match {
      case dot@DOT() =>
        val (r2, (ident, identPos)) = simpleIdentifier(reader.rest)
        dereference_!(r2, FunctionCall(SpecialFunctions.Subscript, Seq(arg, ast.StringLiteral(ident)(identPos)), None)(arg.position, dot.position))
      case open@LBRACKET() =>
        val (r2, index) = expr(reader.rest)
        r2.first match {
          case RBRACKET() =>
            (r2.rest, FunctionCall(SpecialFunctions.Subscript, Seq(arg, index), None)(arg.position, open.position))
          case _ =>
            throw expectedToken(r2, RBRACKET())
        }
      case _ =>
        (reader, arg)
    }
  }

  private def cast(reader: Reader): (Reader, Expression) = {
    val (r2, arg) = dereference(reader)
    cast_!(r2, arg)
  }

  @tailrec
  private def cast_!(reader: Reader, arg: Expression): (Reader, Expression) = {
    reader.first match {
      case op@COLONCOLON() =>
        val (r2, (ident, identPos)) = simpleIdentifier(reader.rest)
        cast_!(r2, FunctionCall(SpecialFunctions.Cast(TypeName(ident)), Seq(arg), None)(arg.position, identPos))
      case _ =>
        (reader, arg)
    }
  }

  private def unary(reader: Reader): (Reader, Expression) = {
    reader.first match {
      case op@(MINUS() | PLUS()) =>
        val (r2, arg) = unary(reader.rest)
        (r2, FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg), None)(op.position, op.position))
      case _ =>
        cast(reader)
    }
  }

  private def exp(reader: Reader): (Reader, Expression) = {
    val (r2, arg) = unary(reader)
    r2.first match {
      case op@CARET() =>
        val (r3, arg2) = exp(r2.rest)
        (r3, FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg, arg2), None)(arg.position, op.position))
      case _ =>
        (r2, arg)
    }
  }

  private def factor(reader: Reader): (Reader, Expression) = {
    val (r2, e) = exp(reader)
    factor_!(r2, e)
  }

  @tailrec
  private def factor_!(reader: Reader, arg: Expression): (Reader, Expression) = {
    reader.first match {
      case op@(STAR() | SLASH() | PERCENT()) =>
        val (r2, arg2) = exp(reader.rest)
        factor_!(r2, FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg, arg2), None)(arg.position, op.position))
      case _ =>
        (reader, arg)
    }
  }

  private def term(reader: Reader): (Reader, Expression) = {
    val (r2, e) = factor(reader)
    term_!(r2, e)
  }

  @tailrec
  private def term_!(reader: Reader, arg: Expression): (Reader, Expression) = {
    reader.first match {
      case op@(PLUS() | MINUS() | PIPEPIPE()) =>
        val (r2, arg2) = factor(reader.rest)
        term_!(r2, FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg, arg2), None)(arg.position, op.position))
      case _ =>
        (reader, arg)
    }
  }

  private def order(reader: Reader): (Reader, Expression) = {
    val (r2, e) = term(reader)
    order_!(r2, e)
  }

  @tailrec
  private def order_!(reader: Reader, arg: Expression): (Reader, Expression) = {
    reader.first match {
      case op@(EQUALS() | LESSGREATER() | LESSTHAN() | LESSTHANOREQUALS() | GREATERTHAN() | GREATERTHANOREQUALS() | EQUALSEQUALS() | BANGEQUALS()) =>
        val (r2, arg2) = term(reader.rest)
        order_!(r2, FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg, arg2), None)(arg.position, op.position))
      case _ =>
        (reader, arg)
    }
  }

  // reader is positioned after the open-paren
  private def parseArgList(reader: Reader, arg0: Option[Expression] = None): (Reader, Seq[Expression]) = {
    reader.first match {
      case RPAREN() =>
        (reader.rest, Nil)
      case _ =>
        val args = Vector.newBuilder[Expression]
        args ++= arg0

        @tailrec
        def loop(reader: Reader): Reader = {
          val (r2, candidate) = expr(reader)
          args += candidate
          r2.first match {
            case RPAREN() =>
              r2.rest
            case COMMA() =>
              loop(r2.rest)
            case _ =>
              throw expectedTokens(r2, Set(RPAREN(), COMMA()))
          }
        }

        val finalReader = loop(reader)

        (finalReader, args.result())
    }
  }

  private def parseIn(name: FunctionName, scrutinee: Expression, op: Token, reader: Reader): (Reader, Expression) = {
    reader.first match {
      case LPAREN() =>
        val (r2, args) = parseArgList(reader.rest, arg0 = Some(scrutinee))
        (r2, FunctionCall(name, args, None)(scrutinee.position, op.position))
      case _ =>
        throw expectedToken(reader, LPAREN())
    }
  }

  private def parseLike(name: FunctionName, scrutinee: Expression, op: Token, reader: Reader): (Reader, Expression) = {
    val (r2, pattern) = likeBetweenIn(reader)
    (r2, FunctionCall(name, Seq(scrutinee, pattern), None)(scrutinee.position, op.position))
  }

  private def parseBetween(name: FunctionName, scrutinee: Expression, op: Token, reader: Reader): (Reader, Expression) = {
    val (r2, lowerBound) = likeBetweenIn(reader)
    r2.first match {
      case AND() =>
        val (r3, upperBound) = likeBetweenIn(r2.rest)
        (r3, FunctionCall(name, Seq(scrutinee, lowerBound, upperBound), None)(scrutinee.position, op.position))
      case _ =>
        throw expectedToken(r2, AND())
    }
  }

  // This is an annoying bit of grammar.  This node is defined as
  //   likeBetweenIn [bunchofcases] | order
  // so we need to rewrite it to eliminate the left-recursion.
  // Again, it becomes
  //    lBI = order lBI'
  //    lBI' = [the various cases] lBI' | ε
  private def likeBetweenIn(reader: Reader): (Reader, Expression) = {
    val (r2, arg) = order(reader)
    likeBetweenIn_!(r2, arg)
  }

  @tailrec
  private def likeBetweenIn_!(reader:Reader, arg: Expression): (Reader, Expression) = {
    reader.first match {
      case is: IS =>
        // This is either IS NULL or IS NOT NULL
        val (r2, arg2) =
          reader.rest.first match {
            case NULL() =>
              (reader.rest.rest, FunctionCall(SpecialFunctions.IsNull, Seq(arg), None)(arg.position, is.position))
            case NOT() =>
              reader.rest.rest.first match {
                case NULL() =>
                  (reader.rest.rest.rest, FunctionCall(SpecialFunctions.IsNotNull, Seq(arg), None)(arg.position, is.position))
                case _ =>
                  throw expectedToken(reader.rest.rest, NULL())
              }
            case _ =>
              throw expectedTokens(reader.rest, Set(NULL(), NOT()))
          }
        likeBetweenIn_!(r2, arg2)
      case like: LIKE =>
        // woo only one possibility!
        val (r2, arg2) = parseLike(SpecialFunctions.Like, arg, like, reader.rest)
        likeBetweenIn_!(r2, arg2)
      case between: BETWEEN =>
        val (r2, arg2) = parseBetween(SpecialFunctions.Between, arg, between, reader.rest)
        likeBetweenIn_!(r2, arg2)
      case not: NOT =>
        // could be NOT BETWEEN, NOT IN, or NOT LIKE
        reader.rest.first match {
          case between: BETWEEN =>
            val (r2, arg2) = parseBetween(SpecialFunctions.NotBetween, arg, not, reader.rest.rest)
            likeBetweenIn_!(r2, arg2)
          case in: IN =>
            val (r2, arg2) = parseIn(SpecialFunctions.NotIn, arg, not, reader.rest.rest)
            likeBetweenIn_!(r2, arg2)
          case like: LIKE =>
            val (r2, arg2) = parseLike(SpecialFunctions.NotLike, arg, not, reader.rest.rest)
            likeBetweenIn_!(r2, arg2)
          case other =>
            throw expectedTokens(reader.rest, Set(BETWEEN(), IN(), LIKE()))
        }
      case in: IN =>
        // Again only one possibility!
        val (r2, arg2) = parseIn(SpecialFunctions.In, arg, in, reader.rest)
        likeBetweenIn_!(r2, arg2)
      case _ =>
        (reader, arg)
    }
  }

  private def negation(reader: Reader): (Reader, Expression) = {
    reader.first match {
      case op: NOT =>
        val (r2, arg) = negation(reader)
        (r2, FunctionCall(SpecialFunctions.Operator(op.printable), Seq(arg), None)(op.position, op.position))
      case _ =>
        likeBetweenIn(reader)
    }
  }

  private def conjunction(reader: Reader): (Reader, Expression) = {
    val (r2, e) = negation(reader)
    conjunction_!(r2, e)
  }

  @tailrec
  private def conjunction_!(reader: Reader, arg: Expression): (Reader, Expression) = {
    reader.first match {
      case and: AND =>
        val (r2, arg2) = negation(reader.rest)
        conjunction_!(r2, FunctionCall(SpecialFunctions.Operator(and.printable), Seq(arg, arg2), None)(arg.position, and.pos))
      case _ =>
        (reader, arg)
    }
  }

  // This is a pattern that'll be used all over this parser.  The
  // combinators approach was
  //    disjunction = (disjunction OR conjunction) | conjunction;
  // which is left-recursive.  We can eliminate that by instead doing
  //    disjunction = conjunction disjunction'
  //    disjunction' = OR conjunction disjunction' | ε
  private def disjunction(reader: Reader): (Reader, Expression) = {
    val (r2, e) = conjunction(reader)
    disjunction_!(r2, e)
  }

  @tailrec
  private def disjunction_!(reader: Reader, arg: Expression): (Reader, Expression) = {
    reader.first match {
      case or: OR =>
        val (r2, arg2) = conjunction(reader.rest)
        disjunction_!(r2, FunctionCall(SpecialFunctions.Operator(or.printable), Seq(arg, arg2), None)(arg.position, or.pos))
      case _ =>
        (reader, arg)
    }
  }

  private def expr(reader: Reader): (Reader, Expression) = {
    disjunction(reader)
  }
}
