package com.socrata.soql.environment

final class HoleName(name: String) extends AbstractName[HoleName](name) {
  protected def hashCodeSeed = 0x5d8634b2
}

object HoleName extends (String => HoleName) {
  def apply(holeName: String) = new HoleName(holeName)
}
