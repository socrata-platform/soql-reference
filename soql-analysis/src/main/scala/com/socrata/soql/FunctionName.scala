package com.socrata.soql

import com.ibm.icu.text.Collator
import com.ibm.icu.util.ULocale
import com.ibm.icu.lang.UCharacter

final class FunctionName(val name: String) extends Ordered[FunctionName] {
  import FunctionName._

  val canonicalName = UCharacter.toLowerCase(locale, name.replaceAll("-","_"))

  // I don't think this is correct; in particular I don't think a == b
  // implies a.hashCode == b.hashCode.
  override def hashCode = canonicalName.hashCode ^ 0xfb392dda

  override def equals(o: Any) = o match {
    case that: FunctionName =>
      collator.compare(this.canonicalName, that.canonicalName) == 0
    case _ => false
  }

  def compare(that: FunctionName) = {
    collator.compare(this.canonicalName, that.canonicalName)
  }

  override def toString = canonicalName
}

object FunctionName extends (String => FunctionName) {
  val locale = ULocale.ENGLISH
  val collator = Collator.getInstance(locale)

  def apply(functionName: String) = new FunctionName(functionName)
}
