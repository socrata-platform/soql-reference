package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, SimpleHierarchyEncodeBuilder, SimpleHierarchyDecodeBuilder, InternalTag}

import com.socrata.soql.ast
import com.socrata.soql.collection.{OrderedMap, OrderedMapHelper}
import com.socrata.soql.environment.{ResourceName, ColumnName, HoleName}
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.BinaryTree

// This class exists purely to be the JSON-serialized form of
// ParsedTableDescriptions, but it can also be used by something that
// has to handle passing through JSONified table descriptions but
// doesn't care about the actual parse tree itself.

sealed trait UnparsedTableDescription[+ResourceNameScope, +ColumnType] {
  private[analyzer2] def parse(parser: Boolean => AbstractParser): ParsedTableDescription[ResourceNameScope, ColumnType]
  private[analyzer2] type SoQL <: String
  private[analyzer2] def soql: SoQL
}

object UnparsedTableDescription {
  implicit def jEncode[RNS: JsonEncode, CT: JsonEncode] =
    SimpleHierarchyEncodeBuilder[UnparsedTableDescription[RNS, CT]](InternalTag("type")).
      branch[Dataset[CT]]("dataset")(Dataset.encode[CT], implicitly).
      branch[Query[RNS, CT]]("query")(Query.encode[RNS, CT], implicitly).
      branch[TableFunction[RNS, CT]]("tablefunc")(TableFunction.encode[RNS, CT], implicitly).
      build

  implicit def jDecode[RNS: JsonDecode, CT: JsonDecode] =
    SimpleHierarchyDecodeBuilder[UnparsedTableDescription[RNS, CT]](InternalTag("type")).
      branch[Dataset[CT]]("dataset")(Dataset.decode[CT], implicitly).
      branch[Query[RNS, CT]]("query")(Query.decode[RNS, CT], implicitly).
      branch[TableFunction[RNS, CT]]("tablefunc")(TableFunction.decode[RNS, CT], implicitly).
      build

  case class Dataset[+ColumnType](
    name: DatabaseTableName,
    schema: OrderedMap[DatabaseColumnName, NameEntry[ColumnType]]
  ) extends UnparsedTableDescription[Nothing, ColumnType] {
    private[analyzer2] def rewriteScopes[RNS, RNS2](scopeMap: Map[RNS, RNS2]) = this
    private[analyzer2] def parse(f: Boolean => AbstractParser): ParsedTableDescription.Dataset[ColumnType] =
      ParsedTableDescription.Dataset(name, schema)
    private[analyzer2] type SoQL = Nothing
    private[analyzer2] def soql = ???
  }
  object Dataset {
    private[UnparsedTableDescription] def encode[CT: JsonEncode]: JsonEncode[Dataset[CT]] =
      new JsonEncode[Dataset[CT]] {
        implicit val schemaEncode = OrderedMapHelper.jsonEncode[DatabaseColumnName, NameEntry[CT]]
        val encoder = AutomaticJsonEncodeBuilder[Dataset[CT]]

        def encode(ds: Dataset[CT]): JValue = encoder.encode(ds)
      }

    private[UnparsedTableDescription] def decode[CT: JsonDecode]: JsonDecode[Dataset[CT]] =
      new JsonDecode[Dataset[CT]] {
        implicit val schemaDecode = OrderedMapHelper.jsonDecode[DatabaseColumnName, NameEntry[CT]]
        val decoder = AutomaticJsonDecodeBuilder[Dataset[CT]]

        def decode(v: JValue) = decoder.decode(v)
      }
  }

  case class Query[+ResourceNameScope, +ColumnType](
    scope: ResourceNameScope, // This scope is to resolve both basedOn and any tables referenced within the soql
    canonicalName: CanonicalName, // This is the canonical name of this query; it is assumed to be unique across scopes
    basedOn: ResourceName,
    soql: String,
    parameters: Map[HoleName, ColumnType]
  ) extends UnparsedTableDescription[ResourceNameScope, ColumnType] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def rewriteScopes[RNS >: ResourceNameScope, RNS2](scopeMap: Map[RNS, RNS2]) =
      copy(scope = scopeMap(scope))
    private[analyzer2] def parse(parser: Boolean => AbstractParser): ParsedTableDescription.Query[ResourceNameScope, ColumnType] =
      ParsedTableDescription.Query(
        scope,
        canonicalName,
        basedOn,
        parser(false).binaryTreeSelect(soql),
        soql,
        parameters
      )
  }
  object Query {
    private[UnparsedTableDescription] def encode[RNS: JsonEncode, CT: JsonEncode] =
      AutomaticJsonEncodeBuilder[Query[RNS, CT]]
    private[UnparsedTableDescription] def decode[RNS: JsonDecode, CT: JsonDecode] =
      AutomaticJsonDecodeBuilder[Query[RNS, CT]]
  }

  case class TableFunction[+ResourceNameScope, +ColumnType](
    scope: ResourceNameScope, // This scope is to resolve any tables referenced within the soql
    canonicalName: CanonicalName, // This is the canonical name of this UDF; it is assumed to be unique across scopes
    soql: String,
    parameters: OrderedMap[HoleName, ColumnType]
  ) extends UnparsedTableDescription[ResourceNameScope, ColumnType] {
    private[analyzer2] type SoQL = String

    private[analyzer2] def rewriteScopes[RNS >: ResourceNameScope, RNS2](scopeMap: Map[RNS, RNS2]) =
      copy(scope = scopeMap(scope))

    private[analyzer2] def parse(parser: Boolean => AbstractParser): ParsedTableDescription.TableFunction[ResourceNameScope, ColumnType] =
      ParsedTableDescription.TableFunction(
        scope,
        canonicalName,
        parser(false).binaryTreeSelect(soql),
        soql,
        parameters
      )
  }

  object TableFunction {
    private[UnparsedTableDescription] def encode[RNS: JsonEncode, CT: JsonEncode]: JsonEncode[TableFunction[RNS, CT]] =
      new JsonEncode[TableFunction[RNS, CT]] {
        implicit val paramsEncode = OrderedMapHelper.jsonEncode[HoleName, CT]
        val encoder = AutomaticJsonEncodeBuilder[TableFunction[RNS, CT]]

        def encode(u: TableFunction[RNS, CT]): JValue = encoder.encode(u)
      }

    private[UnparsedTableDescription] def decode[RNS: JsonDecode, CT: JsonDecode]: JsonDecode[TableFunction[RNS, CT]] =
      new JsonDecode[TableFunction[RNS, CT]] {
        implicit val paramsDecode = OrderedMapHelper.jsonDecode[HoleName, CT]
        val decoder = AutomaticJsonDecodeBuilder[TableFunction[RNS, CT]]

        def decode(v: JValue) = decoder.decode(v)
      }
  }
}

