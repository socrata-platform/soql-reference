package com.socrata.soql.typechecker

trait HasType[-CV, +CT] {
  def typeOf(cv: CV): CT
}

