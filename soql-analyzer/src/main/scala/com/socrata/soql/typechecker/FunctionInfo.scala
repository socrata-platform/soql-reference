package com.socrata.soql.typechecker

import com.socrata.soql.functions.{Function, MonomorphicFunction}
import com.socrata.soql.environment.FunctionName

trait FunctionInfo[Type] {
  def functionsWithArity(name: FunctionName, n: Int): Set[Function[Type]]
}
