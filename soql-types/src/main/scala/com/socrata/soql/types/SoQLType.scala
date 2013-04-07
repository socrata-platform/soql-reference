package com.socrata.soql.types

import com.ibm.icu.util.CaseInsensitiveString

import com.socrata.soql.environment.TypeName

sealed abstract class SoQLAnalysisType(n: String) {
  val name = TypeName(n)

  override final def toString = name.toString
  def isPassableTo(that: SoQLAnalysisType): Boolean = (this == that)

  def real: Boolean
  def canonical: SoQLType
}

sealed abstract class SoQLType(n: String) extends SoQLAnalysisType(n) {
  def real = true
  def canonical: SoQLType = this
}

object SoQLType {
  // FIXME: Figure out a way to DRY this.  It's pretty easy in Scala 2.10, but
  // I still want to retain pre-2.10 compat.
  val typesByName = Seq(
    SoQLID, SoQLText, SoQLBoolean, SoQLNumber, SoQLMoney, SoQLDouble, SoQLFixedTimestamp, SoQLFloatingTimestamp,
    SoQLDate, SoQLTime, SoQLObject, SoQLArray, SoQLLocation, SoQLJson
  ).foldLeft(Map.empty[TypeName, SoQLType]) { (acc, typ) =>
    acc + (typ.name -> typ)
  }
}

case object SoQLID extends SoQLType("row_identifier")
case object SoQLText extends SoQLType("text")
case object SoQLBoolean extends SoQLType("boolean")
case object SoQLNumber extends SoQLType("number")
case object SoQLMoney extends SoQLType("money")
case object SoQLDouble extends SoQLType("double")
case object SoQLFixedTimestamp extends SoQLType("fixed_timestamp")
case object SoQLFloatingTimestamp extends SoQLType("floating_timestamp")
case object SoQLDate extends SoQLType("date")
case object SoQLTime extends SoQLType("time")
case object SoQLObject extends SoQLType("object")
case object SoQLArray extends SoQLType("array")
case object SoQLLocation extends SoQLType("location")
case object SoQLJson extends SoQLType("json")

case object SoQLNull extends SoQLType("null") {
  override def isPassableTo(that: SoQLAnalysisType) = true
}

sealed abstract class FakeSoQLType(name: String) extends SoQLAnalysisType(name) {
  def real = false
  override def isPassableTo(that: SoQLAnalysisType) =
    super.isPassableTo(that) || canonical.isPassableTo(that)
}

case class SoQLTextLiteral(text: CaseInsensitiveString) extends FakeSoQLType("*text") {
  def canonical = SoQLText
}
object SoQLTextLiteral {
  def apply(s: String): SoQLTextLiteral = apply(new CaseInsensitiveString(s))
}

case class SoQLNumberLiteral(number: BigDecimal) extends FakeSoQLType("*number") {
  def canonical = SoQLNumber
}
