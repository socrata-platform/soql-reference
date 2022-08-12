package com.socrata.soql.analyzer2

import com.socrata.prettyprint.prelude._

sealed abstract class TableFunc(val debugDoc: Doc[Nothing])
object TableFunc {
  case object Union extends TableFunc(d"UNION")
  case object UnionAll extends TableFunc(d"UNION ALL")
  case object Intersect extends TableFunc(d"INTERSECT")
  case object IntersectAll extends TableFunc(d"INTERSECT ALL")
  case object Minus extends TableFunc(d"MINUS")
  case object MinusAll extends TableFunc(d"MINUS ALL")
}
