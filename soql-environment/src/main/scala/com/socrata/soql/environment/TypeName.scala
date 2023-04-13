package com.socrata.soql.environment

import com.rojoma.json.v3.util.{WrapperJsonCodec, WrapperFieldCodec}

final class TypeName(name: String) extends AbstractName[TypeName](name) {
  protected def hashCodeSeed = 0x32fa2313
}

object TypeName extends (String => TypeName) {
  def apply(functionName: String) = new TypeName(functionName)

  implicit val jCodec = WrapperJsonCodec[TypeName](this, _.name)
  implicit val fCodec = WrapperFieldCodec[TypeName](this, _.name)
}
