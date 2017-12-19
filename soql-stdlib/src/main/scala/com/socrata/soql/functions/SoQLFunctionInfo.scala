package com.socrata.soql.functions

import com.socrata.soql.typechecker.FunctionInfo
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment.FunctionName

object SoQLFunctionInfo extends FunctionInfo[SoQLType] {
  def functionsWithArity(name: FunctionName, n: Int) =
    SoQLFunctions.nAdicFunctionsByNameThenArity.get(name) match {
      case Some(funcsByArity) =>
        funcsByArity.get(n) match {
          case Some(fs) =>
            fs
          case None =>
            variadicFunctionsWithArity(name, n)
        }
      case None =>
        variadicFunctionsWithArity(name, n)
    }

  def variadicFunctionsWithArity(name: FunctionName, n: Int): Set[Function[SoQLType]] = {
    SoQLFunctions.variadicFunctionsByName.get(name) match {
      case Some(funcs) =>

        funcs.foldLeft(Set.empty[Function[SoQLType]])((acc, fun) => {
          var i = n
          while(i >= 0) {
            if (i == fun.minArity) {
              return acc + fun
            } else {
              i -= fun.minArity
            }
          }

          acc
        })
      case None =>
        Set.empty
    }
  }
}
