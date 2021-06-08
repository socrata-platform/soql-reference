package com.socrata.soql.types

import com.socrata.soql.typechecker.FunctionInfo
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.functions.Function

object TestFunctionInfo extends FunctionInfo[TestType] {
  def functionsWithArity(name: FunctionName, n: Int) =
    TestFunctions.nAdicFunctionsByNameThenArity.get(name) match {
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

  def variadicFunctionsWithArity(name: FunctionName, n: Int): Set[Function[TestType]] = {
    TestFunctions.variadicFunctionsByNameThenMinArity.get(name) match {
      case Some(funcsByArity) =>
        var result = Set.empty[Function[TestType]]
        var i = n
        while(i >= 0) {
          funcsByArity.get(i) match {
            case Some(fs) => result ++= fs.filter(_.willAccept(n))
            case None => // nothing
          }
          i -= 1
        }
        result
      case None =>
        Set.empty
    }
  }

  val windowFunctions = TestFunctions.windowFunctions.map(_.name).toSet
}
