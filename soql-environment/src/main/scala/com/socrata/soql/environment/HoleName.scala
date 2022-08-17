package com.socrata.soql.environment

import com.rojoma.json.v3.util.{WrapperJsonCodec, WrapperFieldCodec}

final class HoleName(name: String) extends AbstractName[HoleName](name) {
  protected def hashCodeSeed = 0x5d8634b2
}

object HoleName extends (String => HoleName) {
  def apply(holeName: String) = new HoleName(holeName)

  implicit val jCodec = WrapperJsonCodec[HoleName](this, _.name)
  implicit val fCodec = WrapperFieldCodec[HoleName](this, _.name)
}
