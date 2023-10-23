package com.socrata.soql

package object analyzer2 {
  type AliasAnalysisError[+RNS] = SoQLAnalyzerError.AliasAnalysisError[RNS]
  type TypecheckError[+RNS] = SoQLAnalyzerError.TypecheckError[RNS]
}
