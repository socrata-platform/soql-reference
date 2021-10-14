package com.socrata.soql.parsing.standalone_exceptions

import scala.util.parsing.input.Position

import com.socrata.soql.parsing.RecursiveDescentParser
import com.socrata.soql.parsing.RecursiveDescentParser.{Reader, ParseException}

case class BadParse(message: String, position: Position) extends Exception(message)

class RecursiveDescentBadParse(val reader: Reader)
    extends BadParse(RecursiveDescentBadParse.msg(reader), reader.first.position)
    with ParseException
{
  override val position = super.position
}

object RecursiveDescentBadParse {
  private def msg(reader: Reader) = {
    val sb = new StringBuilder
    sb.append("Expected")

    def loop(expectations: LazyList[RecursiveDescentParser.Expectation], n: Int): Unit = {
      expectations match {
        case LazyList() =>
          // Uhhh... this shouldn't happen
          sb.append(" nothing")
        case hd #:: LazyList() =>
          if(n == 1) sb.append(" or ")
          else if(n > 1) sb.append(", or ") // oxford comma 4eva
          else sb.append(' ')
          sb.append(hd.printable)
        case hd #:: tl =>
          if(n == 0) sb.append(" one of ")
          else sb.append(", ")
          sb.append(hd.printable)
          loop(tl, n+1)
      }
    }
    loop(reader.alternates.to(LazyList), 0)

    sb.append(", but got ").
      append(reader.first.quotedPrintable).
      toString
  }
}
