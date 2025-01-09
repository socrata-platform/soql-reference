package com.socrata.soql.ast

sealed trait Distinctiveness {
  private[ast] def findIdentsAndLiterals: Seq[String]
}

case object Indistinct extends Distinctiveness {
  private[ast] def findIdentsAndLiterals = Nil
}
case object FullyDistinct extends Distinctiveness {
  private[ast] def findIdentsAndLiterals = Seq("DISTINCT")
}
case class DistinctOn(exprs: Seq[Expression]) extends Distinctiveness {
  private[ast] def findIdentsAndLiterals = Seq("DISTINCT","ON") ++ exprs.flatMap(_.findIdentsAndLiterals)
}
