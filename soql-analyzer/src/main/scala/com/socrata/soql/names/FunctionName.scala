package com.socrata.soql.names

import com.ibm.icu.text.{Normalizer, Collator}
import com.ibm.icu.util.ULocale
import com.ibm.icu.lang.UCharacter

final class FunctionName(val name: String) extends Ordered[FunctionName] {
  import FunctionName._

  val canonicalName = Normalizer.normalize(UCharacter.toLowerCase(locale, name.replaceAll("-","_")), Normalizer.DEFAULT)

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
  def unapply(fn: FunctionName) = Some(fn.canonicalName)
}
