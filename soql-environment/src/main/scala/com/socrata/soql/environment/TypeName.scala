package com.socrata.soql.environment

import com.ibm.icu.lang.UCharacter

final class TypeName(val name: String) extends Ordered[TypeName] {
  private lazy val canonicalName = UCharacter.foldCase(name.replaceAll("-","_"), UCharacter.FOLD_CASE_DEFAULT)
  override lazy val hashCode = canonicalName.hashCode ^ 0x32fa2313

  override def equals(o: Any) = o match {
    case that: TypeName =>
      this.canonicalName.equals(that.canonicalName)
    case _ => false
  }

  def compare(that: TypeName) =
    this.canonicalName.compareTo(that.canonicalName)

  override def toString = name
}

object TypeName extends (String => TypeName) {
  def apply(functionName: String) = new TypeName(functionName)
}
