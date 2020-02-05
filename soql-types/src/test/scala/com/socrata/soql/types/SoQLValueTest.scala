package com.socrata.soql.types

import org.scalatest.FunSuite
import org.scalatest.MustMatchers

class SoQLValueTest extends FunSuite with MustMatchers {
  test("SoQLValue is 1:1 with SoQLType, and its typ fields are accurate") {
    import scala.reflect.runtime.universe._

    // Not sure why this is necessary, but without it, the
    // directKnownSubclasses calls return the empty set (2.10.0)
    typeOf[SoQLValue].toString

    val valueClasses = typeOf[SoQLValue].typeSymbol.asClass.knownDirectSubclasses
    val typeClasses = typeOf[SoQLValue].typeSymbol.asClass.knownDirectSubclasses

    valueClasses must not be ('empty)
    valueClasses.size must equal (typeClasses.size)

    valueClasses.foreach { sym =>
      val companionType = sym.typeSignature.member(Compat.termName("typ")).asMethod.returnType.typeSymbol
      companionType.name.toString must equal (sym.name.toString)
    }
  }

  test("SoQLNumber's equals ignores scale") {
    def bd(s: String) = new java.math.BigDecimal(s)
    SoQLNumber(bd("5.00")) must equal (SoQLNumber(bd("5")))
  }

  test("SoQLNumber's hashCode ignores scala") {
    def bd(s: String) = new java.math.BigDecimal(s)
    SoQLNumber(bd("5.00")).hashCode must equal (SoQLNumber(bd("5")).hashCode)
  }
}
