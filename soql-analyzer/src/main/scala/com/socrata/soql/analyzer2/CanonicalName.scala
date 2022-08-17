package com.socrata.soql.analyzer2

import com.rojoma.json.v3.util.{WrapperJsonCodec, WrapperFieldCodec}

case class CanonicalName(name: String)

object CanonicalName extends (String => CanonicalName) {
  implicit val jCodec = WrapperJsonCodec[CanonicalName](this, _.name)
  implicit val fCodec = WrapperFieldCodec[CanonicalName](this, _.name)
}
