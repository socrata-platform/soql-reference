package com.socrata.soql.environment

import com.ibm.icu.lang.UCharacter

final class FunctionName(val name: String) extends Ordered[FunctionName] {
  lazy val caseFolded = UCharacter.foldCase(name.replaceAll("-","_"), UCharacter.FOLD_CASE_DEFAULT)
  override lazy val hashCode = caseFolded.hashCode ^ 0xfb392dda

  override def equals(o: Any) = o match {
    case that: FunctionName =>
      this.caseFolded.equals(that.caseFolded)
    case _ => false
  }

  def compare(that: FunctionName) =
    this.caseFolded.compareTo(that.caseFolded)

  override def toString = name
}

object FunctionName extends (String => FunctionName) {
  def apply(functionName: String) = new FunctionName(functionName)
}
