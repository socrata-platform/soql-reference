package com.socrata.soql.environment

import com.rojoma.json.v3.util.{WrapperJsonCodec, WrapperFieldCodec}

final class FunctionName(name: String) extends AbstractName[FunctionName](name) {
  protected def hashCodeSeed = 0xfb392dda
}

object FunctionName extends (String => FunctionName) {
  def apply(functionName: String) = new FunctionName(functionName)

  implicit val jCodec = WrapperJsonCodec[FunctionName](this, _.name)
  implicit val fCodec = WrapperFieldCodec[FunctionName](this, _.name)
}
