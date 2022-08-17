package com.socrata.soql.environment

import com.rojoma.json.v3.util.{WrapperJsonCodec, WrapperFieldCodec}

final class ColumnName(name: String) extends AbstractName[ColumnName](name) {
  protected def hashCodeSeed = 0x342a3466
}

object ColumnName extends (String => ColumnName) {
  def apply(columnName: String) = new ColumnName(columnName)

  implicit val jCodec = WrapperJsonCodec[ColumnName](this, _.name)
  implicit val fCodec = WrapperFieldCodec[ColumnName](this, _.name)
}
