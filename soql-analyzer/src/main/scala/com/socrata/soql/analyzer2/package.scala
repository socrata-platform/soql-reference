package com.socrata.soql

package object analyzer2 {
  // type alias for referring to any generic soql error since Scala
  // doesn't have type-parameter defaulting.
  type SoQLError[+RNS] = SoQLAnalyzerError[RNS, SoQLAnalyzerError.Payload]
}
