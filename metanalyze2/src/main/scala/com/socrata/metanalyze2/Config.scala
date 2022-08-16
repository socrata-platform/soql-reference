package com.socrata.metanalyze2

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

case class Config(
  hostport: String,
  username: String,
  password: String,
  database: String
)

object Config {
  implicit val jCodec = AutomaticJsonCodecBuilder[Config]
}
