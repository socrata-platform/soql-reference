package com.socrata.soql.typechecker

import com.socrata.prettyprint.prelude._

trait HasDoc[-CV] {
  def docOf(cv: CV): Doc[Nothing]
}

