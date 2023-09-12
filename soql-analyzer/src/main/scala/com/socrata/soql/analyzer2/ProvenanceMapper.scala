package com.socrata.soql.analyzer2

import com.socrata.soql.environment.Provenance

trait FromProvenance[DatabaseTableNameImpl] {
  def fromProvenance(dtn: Provenance): DatabaseTableName[DatabaseTableNameImpl]
}

trait ToProvenance[DatabaseTableNameImpl] {
  def toProvenance(dtn: DatabaseTableName[DatabaseTableNameImpl]): Provenance
}

trait ProvenanceMapper[DatabaseTableNameImpl] extends FromProvenance[DatabaseTableNameImpl] with ToProvenance[DatabaseTableNameImpl]
