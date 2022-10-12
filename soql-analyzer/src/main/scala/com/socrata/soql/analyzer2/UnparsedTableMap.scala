package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser

class UnparsedTableMap[ResourceNameScope, +ColumnType] private[analyzer2] (private val underlying: Map[ResourceNameScope, Map[ResourceName, UnparsedTableDescription[ResourceNameScope, ColumnType]]]) extends AnyVal with TableMapLike[ResourceNameScope, ColumnType] {
  type Self[RNS, +CT] = UnparsedTableMap[RNS, CT]

  private[analyzer2] def parse(params: AbstractParser.Parameters): Either[LexerParserException, TableMap[ResourceNameScope, ColumnType]] =
    Right(new TableMap(underlying.iterator.map { case (rns, m) =>
      rns -> m.iterator.map { case (rn, utd) =>
        utd.parse(params) match {
          case Right(ptd) => rn -> ptd
          case Left(e) => return Left(e)
        }
      }.toMap
    }.toMap))


  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): UnparsedTableMap[ResourceNameScope, ColumnType] = {
    new UnparsedTableMap(underlying.iterator.map { case (rns, m) =>
      rns -> m.iterator.map { case (rn, ptd) =>
        val newptd = ptd match {
          case UnparsedTableDescription.Dataset(name, canonicalName, schema) =>
            UnparsedTableDescription.Dataset(
              tableName(name),
              canonicalName,
              OrderedMap(schema.iterator.map { case (dcn, ne) =>
                columnName(name, dcn) -> ne
              }.toSeq : _*)
            )
          case other =>
            other
        }
        rn -> newptd
      }.toMap
    }.toMap)
  }
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

