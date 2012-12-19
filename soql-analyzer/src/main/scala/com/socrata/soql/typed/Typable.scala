package com.socrata.soql.typed

trait Typable[+Type] {
  def typ: Type
}

