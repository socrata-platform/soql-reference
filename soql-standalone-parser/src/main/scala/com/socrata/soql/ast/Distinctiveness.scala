package com.socrata.soql.ast

sealed trait Distinctiveness

case object Indistinct extends Distinctiveness
case object FullyDistinct extends Distinctiveness
case class DistinctOn(exprs: Seq[Expression]) extends Distinctiveness
