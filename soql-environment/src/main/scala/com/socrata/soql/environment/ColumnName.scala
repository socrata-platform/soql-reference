package com.socrata.soql.environment

import com.ibm.icu.lang.UCharacter
import com.ibm.icu.text.Normalizer

final class ColumnName(val name: String)(implicit val datasetContext: SchemalessDatasetContext) extends Ordered[ColumnName] {
  val canonicalName = Normalizer.normalize(UCharacter.toLowerCase(datasetContext.locale, name).replaceAll("-","_"), Normalizer.DEFAULT)

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
  def apply(columnName: String)(implicit datasetContext: SchemalessDatasetContext) = new ColumnName(columnName)(datasetContext)
}
