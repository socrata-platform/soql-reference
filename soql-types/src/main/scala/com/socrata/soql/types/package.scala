package com.socrata.soql

package object types {
  @deprecated("SoQLAnalysisType is now an alias for SoQLType", since="2.0.0")
  type SoQLAnalysisType = SoQLType

 def validateYear[A <: { def getYear(): Int }](d: A): A =
    if (d.getYear() == 0) throw new IllegalArgumentException("Year cannot be zero") else d
}
