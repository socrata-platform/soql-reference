package com.socrata.soql.types

import com.ibm.icu.util.CaseInsensitiveString

import com.socrata.soql.environment.TypeName

sealed abstract class SoQLType(val name: TypeName) {
  def this(name: String) = this(TypeName(name))
  override final def toString = name.toString

  def real = true

  def isPassableTo(that: SoQLType): Boolean = (this == that)
  def canonical: SoQLType = this
}

object SoQLType {
  // FIXME: Figure out a way to DRY this
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
  override def isPassableTo(that: SoQLType) = true
}

sealed abstract class FakeSoQLType(name: String) extends SoQLType(name) {
  override def real = false
  def realType: SoQLType
  override def canonical = realType
}

case class SoQLTextLiteral(text: CaseInsensitiveString) extends FakeSoQLType("*text") {
  override def isPassableTo(that: SoQLType) = super.isPassableTo(that) || that == SoQLText
  def realType = SoQLText
}
object SoQLTextLiteral {
  def apply(s: String): SoQLTextLiteral = apply(new CaseInsensitiveString(s))
}

case class SoQLNumberLiteral(number: BigDecimal) extends FakeSoQLType("*number") {
  override def isPassableTo(that: SoQLType) = super.isPassableTo(that) || that == SoQLNumber
  def realType = SoQLNumber
}
