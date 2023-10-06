package com.socrata.soql.util

// An expression wrapped in LazyToString will not be evaluated until
// toString, hashCode, or equals is called on it.  The intent is to
// pass it to logging code so that evaluation can be skipped if the
// log level is such that nothing will be printed.
sealed abstract class LazyToString {
  def indent(n: Int) =
    LazyToString(this.toString.replaceAll("\n", "\n" + " " * n))

  override def equals(o: Any) =
    o match {
      case that: LazyToString => this.toString == that.toString
      case _ => false
    }

  override def hashCode = toString.hashCode
}

object LazyToString {
  def apply[T](x: => Any): LazyToString =
    new LazyToString {
      override lazy val toString = x.toString
    }
}
