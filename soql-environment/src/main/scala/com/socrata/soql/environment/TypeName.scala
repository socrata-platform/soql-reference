package com.socrata.soql.environment

final class TypeName(name: String) extends AbstractName[TypeName](name) {
  protected def hashCodeSeed = 0x32fa2313
}

object TypeName extends (String => TypeName) {
  def apply(functionName: String) = new TypeName(functionName)
}
