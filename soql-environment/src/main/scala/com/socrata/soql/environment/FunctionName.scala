package com.socrata.soql.environment

final class FunctionName(name: String) extends AbstractName[FunctionName](name) {
  protected def hashCodeSeed = 0xfb392dda
}

object FunctionName extends (String => FunctionName) {
  def apply(functionName: String) = new FunctionName(functionName)
}
