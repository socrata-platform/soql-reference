package com.socrata.soql.functions

import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSuite

class SoQLFunctionsTest extends FunSuite with MustMatchers {
  test("all functions have distinct identities") {
    SoQLFunctions.functionsByIdentity.size must equal (SoQLFunctions.allFunctions.size)
  }

  test("fixed-arity and variable-arity functions do not share any names") {
    val sharedNames = SoQLFunctions.nAdicFunctions.map(_.name).toSet intersect SoQLFunctions.variadicFunctions.map(_.name).toSet
    sharedNames must be ('empty)
  }

  test("SoQLFunctions has no Monomorphic accessors") {
    for(potentialAccessor <- SoQLFunctions.potentialAccessors) {
      potentialAccessor.getReturnType must not be (classOf[MonomorphicFunction[_]])
    }
  }
}
