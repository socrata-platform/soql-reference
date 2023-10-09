package com.socrata.soql.sqlizer

trait ExtraContext[Result] {
  def finish(): Result
}
