package com.socrata.soql.functions

import com.socrata.soql.typechecker.FunctionInfo
import com.socrata.soql.types.{SoQLAnalysisType, SoQLType}
import com.socrata.soql.environment.FunctionName

object SoQLFunctionInfo extends FunctionInfo[SoQLAnalysisType] {
  def functionsWithArity(name: FunctionName, n: Int) =
    SoQLFunctions.nAdicFunctionsByNameThenArity.get(name) match {
      case Some(funcsByArity) =>
        funcsByArity.get(n) match {
          case Some(fs) =>
            fs
          case None =>
            variadicFunctionsWithArity(name ,n)
        }
      case None =>
        variadicFunctionsWithArity(name, n)
    }

  def variadicFunctionsWithArity(name: FunctionName, n: Int): Set[Function[SoQLAnalysisType]] = {
    SoQLFunctions.variadicFunctionsByNameThenMinArity.get(name) match {
      case Some(funcsByArity) =>
        var result = Set.empty[Function[SoQLAnalysisType]]
        var i = n
        while(i >= 0) {
          funcsByArity.get(i) match {
            case Some(fs) => result ++= fs
            case None => // nothing
          }
          i -= 1
        }
        result
      case None =>
        Set.empty
    }
  }

  def implicitConversions(from: SoQLAnalysisType, to: SoQLAnalysisType) = SoQLTypeConversions.implicitConversions(from, to)
}
