package com.socrata.soql.analysis.types

import com.socrata.soql.names.TypeName

sealed abstract class SoQLType(val name: TypeName) {
  def this(name: String) = this(TypeName(name))
  override final def toString = name.toString

  def real = true
}

object SoQLType {
  // FIXME: Figure out a way to DRY this
  val typesByName = Seq(
    SoQLText, SoQLBoolean, SoQLNumber, SoQLMoney, SoQLDouble, SoQLFixedTimestamp, SoQLFloatingTimestamp, SoQLObject, SoQLArray, SoQLLocation
  ).foldLeft(Map.empty[TypeName, SoQLType]) { (acc, typ) =>
    acc + (typ.name -> typ)
  }
}

case object SoQLText extends SoQLType("text")
case object SoQLBoolean extends SoQLType("boolean")
case object SoQLNumber extends SoQLType("number")
case object SoQLMoney extends SoQLType("money")
case object SoQLDouble extends SoQLType("double")
case object SoQLFixedTimestamp extends SoQLType("fixed_timestamp")
case object SoQLFloatingTimestamp extends SoQLType("floating_timestamp")
case object SoQLObject extends SoQLType("object")
case object SoQLArray extends SoQLType("array")
case object SoQLLocation extends SoQLType("location")
case object SoQLNull extends SoQLType("null")

sealed abstract class FakeSoQLType(name: String) extends SoQLType(name) {
  override def real = false
}

case object SoQLTextLiteral extends FakeSoQLType("*text")
case object SoQLTextFixedTimestampLiteral extends FakeSoQLType("*text/fixed_timestamp")
case object SoQLTextFloatingTimestampLiteral extends FakeSoQLType("*text/floating_timestamp")

case object SoQLNumberLiteral extends FakeSoQLType("*number")
