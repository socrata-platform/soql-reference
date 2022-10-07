package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}

import com.socrata.soql.ast
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ResourceName, ColumnName, HoleName}
import com.socrata.soql.BinaryTree

sealed trait TableDescription[+ResourceNameScope, +ColumnType] {
  private[analyzer2] def rewriteScopes[RNS >: ResourceNameScope, RNS2](scopeMap: Map[RNS, RNS2]): TableDescription[RNS2, ColumnType]

  def asUnparsedTableDescription: UnparsedTableDescription[ResourceNameScope, ColumnType]
}

object TableDescription {
  private[analyzer2] def jsonEncode[RNS: JsonEncode, CT: JsonEncode] =
    new JsonEncode[TableDescription[RNS, CT]] {
      def encode(x: TableDescription[RNS, CT]) =
        JsonEncode.toJValue(x.asUnparsedTableDescription)
    }

  private[analyzer2] def jsonDecode[RNS: JsonDecode, CT: JsonDecode](parserParameters: AbstractParser.Parameters) =
    new JsonDecode[TableDescription[RNS, CT]] {
      def decode(x: JValue) =
        JsonDecode.fromJValue[UnparsedTableDescription[RNS, CT]](x).flatMap { c =>
          c.parse(parserParameters).left.map { _ =>
            DecodeError.InvalidValue(JString(c.soql)).prefix("soql")
          }
        }
    }

  case class Dataset[+ColumnType](
    name: DatabaseTableName,
    schema: OrderedMap[DatabaseColumnName, NameEntry[ColumnType]]
  ) extends TableDescription[Nothing, ColumnType] {
    private[analyzer2] def rewriteScopes[RNS, RNS2](scopeMap: Map[RNS, RNS2]) = this

    def asUnparsedTableDescription =
      UnparsedTableDescription.Dataset(name, schema)
  }

  case class Query[+ResourceNameScope, +ColumnType](
    scope: ResourceNameScope, // This scope is to resolve both basedOn and any tables referenced within the soql
    canonicalName: CanonicalName, // This is the canonical name of this query; it is assumed to be unique across scopes
    basedOn: ResourceName,
    parsed: BinaryTree[ast.Select],
    unparsed: String,
    parameters: Map[HoleName, ColumnType]
  ) extends TableDescription[ResourceNameScope, ColumnType] {
    private[analyzer2] def rewriteScopes[RNS >: ResourceNameScope, RNS2](scopeMap: Map[RNS, RNS2]) =
      copy(scope = scopeMap(scope))

    def asUnparsedTableDescription =
      UnparsedTableDescription.Query(scope, canonicalName, basedOn, unparsed, parameters)
  }

  case class TableFunction[+ResourceNameScope, +ColumnType](
    scope: ResourceNameScope, // This scope is to resolve any tables referenced within the soql
    canonicalName: CanonicalName, // This is the canonical name of this UDF; it is assumed to be unique across scopes
    parsed: BinaryTree[ast.Select],
    unparsed: String,
    parameters: OrderedMap[HoleName, ColumnType]
  ) extends TableDescription[ResourceNameScope, ColumnType] {
    private[analyzer2] def rewriteScopes[RNS >: ResourceNameScope, RNS2](scopeMap: Map[RNS, RNS2]) =
      copy(scope = scopeMap(scope))

    def asUnparsedTableDescription =
      UnparsedTableDescription.TableFunction(scope, canonicalName, unparsed, parameters)
  }
}

