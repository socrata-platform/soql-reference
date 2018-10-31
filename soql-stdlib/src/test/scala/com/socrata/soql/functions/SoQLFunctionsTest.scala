package com.socrata.soql.functions

import org.scalatest.MustMatchers
import org.scalatest.FunSuite

class SoQLFunctionsTest extends FunSuite with MustMatchers {
  test("all functions have distinct identities") {
    if(SoQLFunctions.functionsByIdentity.size != SoQLFunctions.allFunctions.size) {
      SoQLFunctions.allFunctions.groupBy(_.identity).filter(_._2.size != 1).keySet must equal (Set.empty)
    }

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
