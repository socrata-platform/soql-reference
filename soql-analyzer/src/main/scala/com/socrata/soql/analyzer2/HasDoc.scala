package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.environment.AbstractName

trait HasDoc[-CV] {
  def docOf(cv: CV): Doc[Nothing]
}

trait LowPriorityHasDoc {
  implicit object str extends HasDoc[String] {
    def docOf(s: String) = Doc(s)
  }

  implicit def abstractName[N <: AbstractName[N]]: HasDoc[N] =
    new HasDoc[N] {
      def docOf(n: N) = Doc(n.name)
    }
}

object HasDoc extends LowPriorityHasDoc {
  implicit object nothing extends HasDoc[Nothing] {
    def docOf(s: Nothing) = ???
  }
}
