package com.socrata.soql.parsing

import scala.util.parsing.input.Position

final case class SoQLPosition(line: Int, column: Int, sourceText: String, offset: Int) extends Position {
  protected def lineContents = SoQLPosition.findLineFor(sourceText, offset)
}

object SoQLPosition {
  def findLineFor(source: String, offset: Int): String = {
    val pre = source.substring(0, offset)
    val post = source.substring(offset)
    val first = findLastEOL(pre) + 1
    val last = findFirstEOL(post) + offset
    if(last == -1) source.substring(first)
    else source.substring(first, last)
  }

  def findLastEOL(s: String): Int = {
    var i = s.length - 1
    while(i >= 0) {
      var c = s.charAt(i)
      if(c == '\n') return i
      i -= 1
    }
    i
  }

  def findFirstEOL(s: String): Int = {
    val l = s.length
    var i = 0
    while(i < l) {
      var c = s.charAt(i)
      if(c == '\n') return i
      i += 1
    }
    i
  }
}
