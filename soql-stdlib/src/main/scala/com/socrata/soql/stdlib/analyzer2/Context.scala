package com.socrata.soql.stdlib.analyzer2

import com.rojoma.json.v3.util.{AllowMissing, AutomaticJsonCodec}

@AutomaticJsonCodec
case class Context(
  @AllowMissing("Map.empty")
  system: Map[String, String],
  @AllowMissing("UserParameters.empty")
  user: UserParameters
)

object Context {
  val empty = Context(Map.empty, UserParameters.empty)
}
