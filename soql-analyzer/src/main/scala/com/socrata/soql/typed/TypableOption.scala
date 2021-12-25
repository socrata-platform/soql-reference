package com.socrata.soql.typed

trait TypableOption[+Type] {
  def typ: Option[Type]
}
