package com.socrata.soql.analyzer2

trait HasType[-CV, +CT] {
  def typeOf(cv: CV): CT
}

