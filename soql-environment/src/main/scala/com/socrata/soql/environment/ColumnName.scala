package com.socrata.soql.environment

import com.ibm.icu.lang.UCharacter
import com.ibm.icu.text.Normalizer

final class ColumnName(val name: String) extends Ordered[ColumnName] {
  private lazy val canonicalName = UCharacter.foldCase(name.replaceAll("-", "_"), UCharacter.FOLD_CASE_DEFAULT)
  override lazy val hashCode = canonicalName.hashCode ^ 0x342a3466

  // two column names are the same if they share the same dataset
  // context (which any two names under comparison ought to) and if
  // they are equal following downcasing under that dataset context's
  // locale's rules.
  override def equals(o: Any) = o match {
    case that: ColumnName =>
      this.canonicalName.equals(that.canonicalName)
    case _ => false
  }

  def compare(that: ColumnName) =
    this.canonicalName.compareTo(that.canonicalName)

  override def toString = name
}

object ColumnName extends (String => ColumnName) {
  def apply(columnName: String) = new ColumnName(columnName)
}
