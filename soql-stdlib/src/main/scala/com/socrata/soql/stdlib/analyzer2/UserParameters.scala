package com.socrata.soql.stdlib.analyzer2

import com.rojoma.json.v3.ast.{JString, JValue, JNumber}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{AutomaticJsonCodec, SimpleHierarchyCodecBuilder, InternalTag, NullForNone, AllowMissing}

import org.joda.time.{DateTime, LocalDateTime}

import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2.CanonicalName
import com.socrata.soql.environment.HoleName
import com.socrata.soql.types._

object UserParameters {
  val empty = UserParameters(Map.empty, Map.empty)

  sealed abstract class Value {
    private[UserParameters] def toPossibleValue: analyzer2.UserParameters.PossibleValue[SoQLType, SoQLValue]
  }

  object Value {
    private implicit val dateTimeCodec = new JsonEncode[DateTime] with JsonDecode[DateTime] {
      def encode(t: DateTime) = JString(SoQLFixedTimestamp.StringRep(t))
      def decode(v: JValue) =
        v match {
          case JString(SoQLFixedTimestamp.StringRep(t)) => Right(t)
          case JString(_) => Left(DecodeError.InvalidValue(v))
          case _ => Left(DecodeError.InvalidType(expected = JString, got = v.jsonType))
        }
    }
    private implicit val localDateTimeCodec = new JsonEncode[LocalDateTime] with JsonDecode[LocalDateTime] {
      def encode(t: LocalDateTime) = JString(SoQLFloatingTimestamp.StringRep(t))
      def decode(v: JValue) =
        v match {
          case JString(SoQLFloatingTimestamp.StringRep(t)) => Right(t)
          case JString(_) => Left(DecodeError.InvalidValue(v))
          case _ => Left(DecodeError.InvalidType(expected = JString, got = v.jsonType))
        }
    }

    @AutomaticJsonCodec
    case class Text(@NullForNone value: Option[String]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(s) => analyzer2.UserParameters.Value(SoQLText(s))
          case None => analyzer2.UserParameters.Null(SoQLText)
        }
    }
    @AutomaticJsonCodec
    case class Number(@NullForNone value: Option[JNumber]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(n) => analyzer2.UserParameters.Value(SoQLNumber(n.toJBigDecimal))
          case None => analyzer2.UserParameters.Null(SoQLNumber)
        }
    }
    @AutomaticJsonCodec
    case class Bool(@NullForNone value: Option[Boolean]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(b) => analyzer2.UserParameters.Value(SoQLBoolean(b))
          case None => analyzer2.UserParameters.Null(SoQLBoolean)
        }
    }
    @AutomaticJsonCodec
    case class FixedTimestamp(@NullForNone value: Option[DateTime]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(ts) => analyzer2.UserParameters.Value(SoQLFixedTimestamp(ts))
          case None => analyzer2.UserParameters.Null(SoQLFixedTimestamp)
        }
    }
    @AutomaticJsonCodec
    case class FloatingTimestamp(@NullForNone value: Option[LocalDateTime]) extends Value {
      private[UserParameters] override def toPossibleValue =
        value match {
          case Some(ts) => analyzer2.UserParameters.Value(SoQLFloatingTimestamp(ts))
          case None => analyzer2.UserParameters.Null(SoQLFloatingTimestamp)
        }
    }

    implicit val jCodec = SimpleHierarchyCodecBuilder[Value](InternalTag("type"))
      .branch[Text]("text")
      .branch[Number]("number")
      .branch[Bool]("boolean")
      .branch[FixedTimestamp]("fixed_timestamp")
      .branch[FloatingTimestamp]("floating_timestamp")
      .build
  }
}

@AutomaticJsonCodec
case class UserParameters(
  @AllowMissing("Map.empty")
  qualified: Map[CanonicalName, Map[HoleName, UserParameters.Value]],
  @AllowMissing("Map.empty")
  unqualified: Map[HoleName, UserParameters.Value]
) {
  def toUserParameters: analyzer2.UserParameters[SoQLType, SoQLValue] = {
    analyzer2.UserParameters(
      qualified.iterator.map { case (cn, m) =>
        cn -> m.iterator.map { case (hn, v) => hn -> v.toPossibleValue }.toMap
      }.toMap,
      unqualified.iterator.map { case (hn, v) => hn -> v.toPossibleValue }.toMap
    )
  }
}
