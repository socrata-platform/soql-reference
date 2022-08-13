package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

object DocUtils {
  implicit class AugmentedDoc[A](private val underlying: Doc[A]) extends AnyVal {
    def encloseNesting[B >: A](left: Doc[B], right: Doc[B], indent: Int = 2): Doc[B] =
      Seq(underlying).encloseNesting(left, Doc.empty, right, indent)

    def encloseHanging[B >: A](left: Doc[B], right: Doc[B], indent: Int = 2): Doc[B] =
      Seq(underlying).encloseHanging(left, Doc.empty, right, indent)
  }

  implicit class AugmentedDocOpt[A](private val underlying: Option[Doc[A]]) extends AnyVal {
    def encloseNesting[B >: A](left: Doc[B], right: Doc[B], indent: Int = 2): Doc[B] =
      underlying.toSeq.encloseNesting(left, Doc.empty, right, indent)

    def encloseHanging[B >: A](left: Doc[B], right: Doc[B], indent: Int = 2): Doc[B] =
      underlying.toSeq.encloseHanging(left, Doc.empty, right, indent)
  }
}
