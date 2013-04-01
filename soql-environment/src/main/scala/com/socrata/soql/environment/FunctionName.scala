package com.socrata.soql.environment

import com.ibm.icu.lang.UCharacter

final class FunctionName(val name: String) extends Ordered[FunctionName] {
  private lazy val canonicalName = UCharacter.foldCase(name.replaceAll("-","_"), UCharacter.FOLD_CASE_DEFAULT)
  override lazy val hashCode = canonicalName.hashCode ^ 0xfb392dda

  override def equals(o: Any) = o match {
    case that: FunctionName =>
      this.canonicalName.equals(that.canonicalName)
    case _ => false
  }

  def compare(that: FunctionName) =
    this.canonicalName.compareTo(that.canonicalName)

  override def toString = name
}

object FunctionName extends (String => FunctionName) {
  def apply(functionName: String) = new FunctionName(functionName)
}
