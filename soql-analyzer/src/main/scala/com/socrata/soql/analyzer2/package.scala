package com.socrata.soql

package object analyzer2 {
  // type alias for referring to any generic soql error since Scala
  // doesn't have type-parameter defaulting.
  type SoQLError[+RNS] = SoQLAnalyzerError[RNS, SoQLAnalyzerError.Payload]

  // And some handy type aliases for specific sorts of SoQLErrors
  type TableFinderError[+RNS] = SoQLAnalyzerError.TextualError[RNS, SoQLAnalyzerError.TableFinderError]
  type ParserError[+RNS] = SoQLAnalyzerError.TextualError[RNS, SoQLAnalyzerError.ParserError]

  // This one isn't specifically a TextualError because analysis is
  // where "wrong parameters provided" comes out.
  type AnalysisError[+RNS] = SoQLAnalyzerError[RNS, SoQLAnalyzerError.AnalysisError]

  type AliasAnalysisError[+RNS] = SoQLAnalyzerError.TextualError[RNS, SoQLAnalyzerError.AnalysisError.AliasAnalysisError]
  type TypecheckError[+RNS] = SoQLAnalyzerError.TextualError[RNS, SoQLAnalyzerError.AnalysisError.TypecheckError]
}
