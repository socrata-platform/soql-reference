package com.socrata.soql.environment

import com.rojoma.json.v3.util.{WrapperJsonCodec, WrapperFieldCodec}

final class ResourceName(name: String) extends AbstractName[ResourceName](name) {
  protected def hashCodeSeed = 0x342a3467
}

object ResourceName extends (String => ResourceName) {
  def apply(resourceName: String) = new ResourceName(resourceName)

  implicit val jCodec = WrapperJsonCodec[ResourceName](this, _.name)
  implicit val fCodec = WrapperFieldCodec[ResourceName](this, _.name)
}
