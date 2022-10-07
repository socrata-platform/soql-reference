package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}

import com.socrata.soql.environment.ResourceName
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser

class UnparsedTableMap[ResourceNameScope, +ColumnType] private[analyzer2] (private val underlying: Map[ResourceNameScope, Map[ResourceName, UnparsedTableDescription[ResourceNameScope, ColumnType]]]) extends AnyVal {
  private[analyzer2] def parse(params: AbstractParser.Parameters): Either[LexerParserException, TableMap[ResourceNameScope, ColumnType]] =
    Right(new TableMap(underlying.iterator.map { case (rns, m) =>
      rns -> m.iterator.map { case (rn, utd) =>
        utd.parse(params) match {
          case Right(ptd) => rn -> ptd
          case Left(e) => return Left(e)
        }
      }.toMap
    }.toMap))
}

object UnparsedTableMap {
  implicit def jsonEncode[RNS: JsonEncode, CT: JsonEncode]: JsonEncode[UnparsedTableMap[RNS, CT]] =
    new JsonEncode[UnparsedTableMap[RNS, CT]] {
      def encode(t: UnparsedTableMap[RNS, CT]) = JsonEncode.toJValue(t.underlying.toSeq)
    }

  implicit def jsonDecode[RNS: JsonDecode, CT: JsonDecode]: JsonDecode[UnparsedTableMap[RNS, CT]] =
    new JsonDecode[UnparsedTableMap[RNS, CT]] {
      def decode(v: JValue) =
        JsonDecode.fromJValue[Seq[(RNS, Map[ResourceName, UnparsedTableDescription[RNS, CT]])]](v).map { fields =>
          new UnparsedTableMap(fields.toMap)
        }
    }
}

