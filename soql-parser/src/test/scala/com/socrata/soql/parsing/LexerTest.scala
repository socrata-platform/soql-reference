package com.socrata.soql.parsing

import org.scalatest._
import org.scalatest.matchers.MustMatchers

import com.socrata.soql.tokens._

class LexerTest extends WordSpec with MustMatchers {
  def lexTest(s: String, ts: (Token, Int, Int, Int)*) = {
    val tokens = new LexerReader(s).toStream
    tokens must equal (ts.map(_._1))
    tokens.map(_.position) must equal (ts.map { case (_, r,c,o) => new SoQLPosition(r, c, s, o) })
  }

  def lex(s: String) {
    new LexerReader(s).toStream.force
  }

  "Lexing" should {
    "set positions on identifiers" in {
      lexTest("hello there\n  world",
              (Identifier("hello", false), 1, 1, 0),
              (Identifier("there", false), 1, 7, 6),
              (Identifier("world", false), 2, 3, 14))
    }

    "set positions on system identifiers" in {
      lexTest(":hello :there\n  :world",
              (SystemIdentifier(":hello", false), 1, 1, 0),
              (SystemIdentifier(":there", false), 1, 8, 7),
              (SystemIdentifier(":world", false), 2, 3, 16))
    }

    "read quoted identifiers as identifiers, not keywords" in {
      lexTest("`select`", (Identifier("select", true), 1, 1, 0))
    }

    "forbid newlines in java-style strings" in {
      evaluating { lex("\"\n\"") } must produce[UnterminatedString]
    }

    "allow newlines in sql-style strings" in {
      lexTest("'\n'", (StringLiteral("\n"), 1, 1, 0))
    }

    "interpret double-single-quote as single-quotes in sql-style strings" in {
      lexTest("'hello''world'", (StringLiteral("hello'world"), 1, 1, 0))
    }

    "ignore escapes in sql-style strings" in {
      lexTest("'hello\\nworld'", (StringLiteral("hello\\nworld"), 1, 1, 0))
    }

    "interpret escapes in sql-style strings" in {
      lexTest("\"hello\\nworld\"", (StringLiteral("hello\nworld"), 1, 1, 0))
    }
  }
}
