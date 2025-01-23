package com.socrata.soql.analyzer2

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AllowMissing}

import com.socrata.soql.environment.{ResourceName, HoleName, ColumnName}
import com.socrata.soql.parsing.AbstractParser

private[analyzer2] case class EncodableParameters(
  allowJoins: Boolean,
  systemColumnAliasesAllowed: Set[ColumnName],
  allowJoinFunctions: Boolean,
  @AllowMissing("false")
  allowInSubselect: Boolean
) {
  def toParameters =
    AbstractParser.Parameters(
      allowJoins = allowJoins,
      systemColumnAliasesAllowed = systemColumnAliasesAllowed,
      allowJoinFunctions = allowJoinFunctions,
      allowInSubselect = allowInSubselect,
      allowHoles = false
    )
}

private[analyzer2] object EncodableParameters {
  implicit val paramsCodec = AutomaticJsonCodecBuilder[EncodableParameters]
  def fromParams(params: AbstractParser.Parameters) = {
    val AbstractParser.Parameters(allowJoins, systemColumnAliasesAllowed, allowJoinFunctions, _allowHoles, allowInSubselect) = params
    EncodableParameters(allowJoins, systemColumnAliasesAllowed, allowJoinFunctions, allowInSubselect)
  }
}
