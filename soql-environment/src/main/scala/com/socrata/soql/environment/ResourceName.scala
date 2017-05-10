package com.socrata.soql.environment


final class ResourceName(name: String) extends AbstractName[ResourceName](name) {
  protected def hashCodeSeed = 0x342a3467
}

object ResourceName extends (String => ResourceName) {
  def apply(resourceName: String) = new ResourceName(resourceName)
}
