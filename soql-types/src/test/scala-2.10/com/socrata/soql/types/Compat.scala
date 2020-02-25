package com.socrata.soql.types

import scala.reflect.runtime.universe._

object Compat {
  def termName(n: String) = newTermName(n)
}

