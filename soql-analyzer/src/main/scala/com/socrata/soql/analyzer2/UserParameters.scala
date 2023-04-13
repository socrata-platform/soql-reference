package com.socrata.soql.analyzer2

import com.socrata.soql.environment.HoleName

case class UserParameters[+CT, +CV](
  qualified: Map[CanonicalName, Map[HoleName, UserParameters.PossibleValue[CT, CV]]],
  // This is used for a top-level (i.e., non-saved, anonymous) query
  // that contains param references to figure out what an unqualified
  // `param("whatever")` form refers to in anonymous SoQL
  unqualified: Map[HoleName, UserParameters.PossibleValue[CT, CV]] =
    Map.empty[HoleName, Nothing]
)

object UserParameters {
  val empty = UserParameters[Nothing, Nothing](Map.empty, Map.empty)

  def emptyFor[MT <: MetaTypes](map: FoundTables[MT]) = {
    val UserParameterSpecs(qualifiedSpecs, anonymousSpecs) = map.knownUserParameters
    val qualified = qualifiedSpecs.iterator.map { case (cn, hnct) =>
      cn -> hnct.iterator.map { case (hn, ct) => hn -> Null(ct) }.toMap
    }.toMap
    val anonymous = anonymousSpecs match {
      case Right(hnct) =>
        hnct.iterator.map { case (hn, ct) => hn -> Null(ct) }.toMap
      case Left(_) =>
        Map.empty[HoleName, PossibleValue[MT#ColumnType, Nothing]]
    }
    UserParameters(qualified, anonymous)
  }

  sealed abstract class PossibleValue[+CT, +CV]
  case class Value[+CV](value: CV) extends PossibleValue[Nothing, CV]
  case class Null[+CT](typ: CT) extends PossibleValue[CT, Nothing]
}
