package com.socrata.soql

import scala.reflect.ClassTag
import scala.util.parsing.input.{Position, NoPosition}

import com.rojoma.json.v3.ast.{JNull, JString, JValue, JObject}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.matcher.{Variable, PObject}
import com.rojoma.json.v3.util.{SimpleHierarchyCodecBuilder, SimpleHierarchyEncodeBuilder, SimpleHierarchyDecodeBuilder}

import com.socrata.soql.parsing.SoQLPosition

package object jsonutils {
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
            SoQLPosition(row(results), col(results), text(results), col(results) - 1)
          }
      }
  }

  private[soql] class SingletonCodec[T](value: T) extends JsonEncode[T] with JsonDecode[T] {
    def encode(t: T) = JObject.canonicalEmpty
    def decode(j: JValue) =
      j match {
        case obj: JObject => Right(value)
        case other => Left(DecodeError.InvalidType(expected = JObject, got = other.jsonType))
      }
  }

  private[soql] object HierarchyImplicits {
    implicit class AugmentedSHCB[T <: AnyRef](private val underlying: SimpleHierarchyCodecBuilder[T]) extends AnyVal {
      def singleton[U <: T : ClassTag](tag: String, value: U) = {
        implicit val c = new SingletonCodec(value)
        underlying.branch[U](tag)
      }
    }
    implicit class AugmentedSHEB[T <: AnyRef](private val underlying: SimpleHierarchyEncodeBuilder[T]) extends AnyVal {
      def singleton[U <: T : ClassTag](tag: String, value: U) = {
        implicit val c = new SingletonCodec(value)
        underlying.branch[U](tag)
      }
    }
    implicit class AugmentedSHDB[T <: AnyRef](private val underlying: SimpleHierarchyDecodeBuilder[T]) extends AnyVal {
      def singleton[U <: T : ClassTag](tag: String, value: U) = {
        implicit val c = new SingletonCodec(value)
        underlying.branch[U](tag)
      }
    }
  }
}
