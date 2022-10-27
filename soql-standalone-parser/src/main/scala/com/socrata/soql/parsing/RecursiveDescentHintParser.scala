package com.socrata.soql.parsing

import scala.annotation.tailrec
import scala.util.parsing.input.Position
import com.socrata.soql.ast.{CompoundRollup, Hint, Materialized, NoChainMerge, NoRollup, RollupAtJoin}
import com.socrata.soql.parsing.RecursiveDescentParser.{AHint, COMPOUND_ROLLUP, ParseResult, Reader}
import com.socrata.soql.tokens.{COMMA, LPAREN, RPAREN}

trait RecursiveDescentHintParser { this: RecursiveDescentParser =>

  protected final def hints(reader: Reader): ParseResult[Seq[Hint]] = {
    reader.first match {
      case RecursiveDescentParser.HINT() =>
        reader.rest.first match {
          case LPAREN() =>
            parseHintList(reader.rest.rest)
          case _ =>
            fail(reader.rest, LPAREN())
        }
      case _ =>
        ParseResult(reader, Seq.empty)
    }
  }

  protected final def hint(reader: Reader): ParseResult[(Hint, Position)] =
    reader.first match {
      case x@RecursiveDescentParser.MATERIALIZED() =>
        ParseResult(reader.rest, (Materialized(x.position), x.position))
      case x@RecursiveDescentParser.NO_ROLLUP() =>
        ParseResult(reader.rest, (NoRollup(x.position), x.position))
      case x@RecursiveDescentParser.NO_CHAIN_MERGE() =>
        ParseResult(reader.rest, (NoChainMerge(x.position), x.position))
      case x@RecursiveDescentParser.COMPOUND_ROLLUP() =>
        ParseResult(reader.rest, (CompoundRollup(x.position), x.position))
      case x@RecursiveDescentParser.ROLLUP_AT_JOIN() =>
        ParseResult(reader.rest, (RollupAtJoin(x.position), x.position))
      case _ => fail(reader, AHint)
    }

  private def parseHintList(reader: Reader): ParseResult[Seq[Hint]] = {
    reader.first match {
      case RPAREN() =>
        ParseResult(reader.rest, Nil)
      case _ =>
        val args = Vector.newBuilder[Hint]
        @tailrec
        def loop(reader: Reader): Reader = {
          val ParseResult(r2, candidate) = hint(reader)
          args += candidate._1
          r2.first match {
            case RPAREN() =>
              r2.rest
            case COMMA() =>
              loop(r2.rest)
            case _ =>
              fail(r2, COMMA(), RPAREN())
          }
        }

        val finalReader = loop(reader)

        ParseResult(finalReader, args.result())
    }
  }
}
