package com.socrata.soql.ast

object AST {
  // This controls whether AST nodes print as case classes or as SoQL
  val pretty = true

  private[ast] def unpretty(x: Product) = x.productIterator.mkString(x.productPrefix + "(", ",", ")")
}
