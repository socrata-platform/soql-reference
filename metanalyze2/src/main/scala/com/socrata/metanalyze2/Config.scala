package com.socrata.metanalyze2

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKey}

case class Database(
  hostport: String,
  username: String,
  password: String,
  database: String
)

object Database {
  implicit val jCodec = AutomaticJsonCodecBuilder[Database]
}

case class Overrides(
  parameters: Map[String, Map[String, String]], // uid to parameter-name to parameter-type
  soqlPatch: Map[String, Seq[(String, String)]]
)

object Overrides {
  implicit val jCodec = AutomaticJsonCodecBuilder[Overrides]
}

case class Config(
  database: Database,
  overrides: Overrides
)

object Config {
  implicit val jCodec = AutomaticJsonCodecBuilder[Config]
}
