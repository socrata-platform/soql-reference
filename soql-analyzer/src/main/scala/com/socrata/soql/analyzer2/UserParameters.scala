package com.socrata.soql.analyzer2

import com.socrata.soql.environment.HoleName

case class UserParameters[+CT, +CV](
  qualified: Map[CanonicalName, Map[HoleName, UserParameters.PossibleValue[CT, CV]]],
  // This is used for a top-level (i.e., non-saved, anonymous) query
  // that contains param references to figure out what an unqualified
  // `param("whatever")` form refers to.  If it's Left, it's used as a
  // key into the qualified map.  If it's Right, it's used as a
  // parameter directory directly.
  unqualified: Either[CanonicalName, Map[HoleName, UserParameters.PossibleValue[CT, CV]]] =
    Right(Map.empty[HoleName, Nothing])
)
object UserParameters {
  val empty = UserParameters[Nothing, Nothing](Map.empty, Right(Map.empty))

  def emptyFor[RNS, CT](map: FoundTables[RNS, CT]) = {
    val known = map.knownUserParameters.iterator.map { case (cn, hnct) =>
      cn -> hnct.iterator.map { case (hn, ct) => hn -> Null(ct) }.toMap
    }.toMap
    UserParameters(known)
  }

  sealed abstract class PossibleValue[+CT, +CV]
  case class Value[+CV](value: CV) extends PossibleValue[Nothing, CV]
  case class Null[+CT](typ: CT) extends PossibleValue[CT, Nothing]
}
