package com.socrata.soql

import com.ibm.icu.lang.UCharacter

final class ColumnName(val name: String)(implicit val datasetContext: DatasetContext) extends Ordered[ColumnName] {
  val canonicalName = UCharacter.toLowerCase(datasetContext.locale, name).replaceAll("-","_")

  // I don't think this is correct; in particular I don't think a == b
  // implies a.hashCode == b.hashCode.
  override def hashCode = canonicalName.hashCode ^ datasetContext.hashCode ^ 0x342a3466

  // two column names are the same if they share the same dataset
  // context (which any two names under comparison ought to) and if
  // they are equal following downcasing under that dataset context's
  // locale's rules.
  override def equals(o: Any) = o match {
    case that: ColumnName =>
      this.datasetContext == that.datasetContext && datasetContext.collator.compare(this.canonicalName, that.canonicalName) == 0
    case _ => false
  }

  def compare(that: ColumnName) = {
    if(this.datasetContext != that.datasetContext) throw new IllegalArgumentException("Cannot compare two columns from different dataset contexts")
    datasetContext.collator.compare(this.canonicalName, that.canonicalName)
  }

  override def toString = canonicalName
}

object ColumnName {
  def apply(columnName: String)(implicit datasetContext: DatasetContext) = new ColumnName(columnName)(datasetContext)
}
