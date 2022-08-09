package com.socrata.soql.analyzer2

sealed abstract class TableFunc
object TableFunc {
  case object Union extends TableFunc
  case object UnionAll extends TableFunc
  case object Intersect extends TableFunc
  case object IntersectAll extends TableFunc
  case object Minus extends TableFunc
  case object MinusAll extends TableFunc
}
