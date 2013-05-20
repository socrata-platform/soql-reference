package com.socrata.soql.types.obfuscation

object Quadifier {
  private val digitMask = ~((-1L) << 5)
  private val alphabet = Array(
    '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
    'i', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
  )
  private val inverseAlphabetLength = 128
  private val inverseAlphabet = {
    val arr = new Array[Int](inverseAlphabetLength)
    for(i <- 0 until inverseAlphabetLength) arr(i) = alphabet.indexOf(i.toChar)
    arr
  }

  /** A string that defines a regex that matches a quad. */
  val ParseRegex = "([" + alphabet.mkString + "]{4})"

  /** Converts the 20 least-significant bits of its parameter into a string.
    * @param n
    */
  def quadify(n: Int): String = {
    val cs = new Array[Char](4)
    quadify(n, cs, 0)
    new String(cs)
  }

  def quadify(n: Int, cs: Array[Char], offset: Int = 0) {
    cs(offset) = alphabet(((n >> 15) & digitMask).toInt)
    cs(offset+1) = alphabet(((n >> 10) & digitMask).toInt)
    cs(offset+2) = alphabet(((n >> 5) & digitMask).toInt)
    cs(offset+3) = alphabet((n & digitMask).toInt)
  }

  def dequadify(s: String, offset: Int = 0): Option[Int] =
    try {
      Some(dequadifyEx(s, offset))
    } catch {
      case _: IllegalArgumentException =>
        None
    }

  def dequadifyEx(s: String, offset: Int = 0): Int = {
    if(s.length < offset + 4) badQuad()
    (deDigit(s.charAt(offset)) << 15) +
      (deDigit(s.charAt(offset + 1)) << 10) +
      (deDigit(s.charAt(offset + 2)) << 5) +
      (deDigit(s.charAt(offset + 3)))
  }

  def isQuad(s: String, offset: Int = 0): Boolean =
    (s.length >= offset + 4) &&
      isDigit(s.charAt(offset)) &&
      isDigit(s.charAt(offset + 1)) &&
      isDigit(s.charAt(offset + 2)) &&
      isDigit(s.charAt(offset + 3))

  private def isDigit(c: Char): Boolean =
    c < inverseAlphabetLength && inverseAlphabet(c) != -1

  private def badQuad() =
    throw new IllegalArgumentException("Not a valid quad")

  private def deDigit(c: Char): Int = {
    if(c >= inverseAlphabetLength) badQuad()
    val digitValue = inverseAlphabet(c)
    if(digitValue == -1) badQuad()
    digitValue
  }
}
