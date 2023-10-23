package com.socrata.soql

import scala.util.parsing.input.{Position, NoPosition}

import com.rojoma.json.v3.ast.{JNull, JString, JValue}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.matcher.{Variable, PObject}

import com.socrata.soql.parsing.SoQLPosition

package object util {
  implicit object PositionCodec extends JsonEncode[Position] with JsonDecode[Position] {
    private val row = Variable[Int]()
    private val col = Variable[Int]()
    private val text = Variable[String]()
    private val pattern =
      PObject(
        "row" -> row,
        "column" -> col,
        "text" -> text
      )

    def encode(p: Position) =
      p match {
        case NoPosition =>
          JNull
        case other =>
          pattern.generate(row := p.line, col := p.column, text := p.longString.split('\n')(0))
      }

    def decode(v: JValue) =
      v match {
        case JNull =>
          Right(NoPosition)
        case other =>
          pattern.matches(v).map { results =>
            SoQLPosition(row(results), col(results), text(results), col(results))
          }
      }
  }
}
