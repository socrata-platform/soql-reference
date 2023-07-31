package com.socrata.soql.analyzer2

import scala.collection.compat._

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, SimpleHierarchyEncodeBuilder, SimpleHierarchyDecodeBuilder, InternalTag}

import com.socrata.soql.ast
import com.socrata.soql.collection.{OrderedMap, OrderedMapHelper}
import com.socrata.soql.environment.{ResourceName, ColumnName, HoleName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.BinaryTree
import com.socrata.soql.analyzer2

// This class exists purely to be the JSON-serialized form of
// TableDescriptions, but it can also be used by something that
// has to handle passing through JSONified table descriptions but
// doesn't care about the actual parse tree itself.

sealed trait UnparsedTableDescription[MT <: MetaTypes] extends TableDescriptionLike with LabelUniverse[MT] {
  private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
    tableName: DatabaseTableName => types.DatabaseTableName[MT2],
    columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
  )(implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, MT2]): UnparsedTableDescription[MT2]
}

object UnparsedTableDescription {
  implicit def jEncode[MT <: MetaTypes](implicit encRNS: JsonEncode[MT#ResourceNameScope], encCT: JsonEncode[MT#ColumnType], encDTN: JsonEncode[MT#DatabaseTableNameImpl], encDCN: JsonEncode[MT#DatabaseColumnNameImpl]) =
    SimpleHierarchyEncodeBuilder[UnparsedTableDescription[MT]](InternalTag("type")).
      branch[Dataset[MT]]("dataset")(Dataset.encode[MT], implicitly).
      branch[Query[MT]]("query")(Query.encode[MT], implicitly).
      branch[TableFunction[MT]]("tablefunc")(TableFunction.encode[MT], implicitly).
      build

  implicit def jDecode[MT <: MetaTypes](implicit decRNS: JsonDecode[MT#ResourceNameScope], decCT: JsonDecode[MT#ColumnType], decDTN: JsonDecode[MT#DatabaseTableNameImpl], decDCN: JsonDecode[MT#DatabaseColumnNameImpl]) =
    SimpleHierarchyDecodeBuilder[UnparsedTableDescription[MT]](InternalTag("type")).
      branch[Dataset[MT]]("dataset")(Dataset.decode[MT], implicitly).
      branch[Query[MT]]("query")(Query.decode[MT], implicitly).
      branch[TableFunction[MT]]("tablefunc")(TableFunction.decode[MT], implicitly).
      build

  case class Dataset[MT <: MetaTypes](
    name: DatabaseTableName[MT#DatabaseTableNameImpl],
    canonicalName: CanonicalName,
    columns: OrderedMap[types.DatabaseColumnName[MT], types.TableDescription.DatasetColumnInfo[MT]],
    ordering: Seq[TableDescription.Ordering[MT]],
    primaryKeys: Seq[Seq[types.DatabaseColumnName[MT]]]
  ) extends UnparsedTableDescription[MT] with TableDescriptionLike.Dataset[MT] {
    val hiddenColumns = columns.values.flatMap { case TableDescription.DatasetColumnInfo(name, _, hidden) =>
      if(hidden) Some(name) else None
    }.to(Set)

    // TODO: Make this turn into an InvalidValue decode error if it
    // fires while json-decoding
    require(ordering.forall { o => columns.contains(o.column) })

    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]) = this

    private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
      tableName: DatabaseTableName => types.DatabaseTableName[MT2],
      columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
    )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): Dataset[MT2] = {
      Dataset(
        tableName(name),
        canonicalName,
        OrderedMap(
          columns.iterator.map { case (dcn, ne) =>
            columnName(name, dcn) -> TableDescription.DatasetColumnInfo(ne.name, ev.convertCT(ne.typ), ne.hidden)
          }.toSeq : _*
        ),
        ordering.map { case TableDescription.Ordering(dcn, ascending, nullLast) =>
          TableDescription.Ordering(columnName(name, dcn), ascending, nullLast)
        },
        primaryKeys.map(_.map(columnName(name, _)))
      )
    }
  }
  object Dataset {
    private[UnparsedTableDescription] def encode[MT <: MetaTypes](implicit encCT: JsonEncode[MT#ColumnType], encDTN: JsonEncode[MT#DatabaseTableNameImpl], encDCN: JsonEncode[MT#DatabaseColumnNameImpl]): JsonEncode[Dataset[MT]] =
      new JsonEncode[Dataset[MT]] with LabelUniverse[MT] {
        implicit val schemaEncode = OrderedMapHelper.jsonEncode[DatabaseColumnName, TableDescription.DatasetColumnInfo[CT]]
        val encoder = AutomaticJsonEncodeBuilder[Dataset[MT]]

        def encode(ds: Dataset[MT]): JValue = encoder.encode(ds)
      }

    private[UnparsedTableDescription] def decode[MT <: MetaTypes](implicit decCT: JsonDecode[MT#ColumnType], decDTN: JsonDecode[MT#DatabaseTableNameImpl], decDCN: JsonDecode[MT#DatabaseColumnNameImpl]): JsonDecode[Dataset[MT]] =
      new JsonDecode[Dataset[MT]] with LabelUniverse[MT] {
        implicit val schemaDecode = OrderedMapHelper.jsonDecode[DatabaseColumnName, TableDescription.DatasetColumnInfo[CT]]
        val decoder = AutomaticJsonDecodeBuilder[Dataset[MT]]

        def decode(v: JValue) = decoder.decode(v)
      }
  }

  sealed trait SoQLUnparsedTableDescription[MT <: MetaTypes] extends UnparsedTableDescription[MT] {
    def soql: String
    private[analyzer2] def parse(params: AbstractParser.Parameters): Either[LexerParserException, TableDescription[MT]]
  }

  case class Query[MT <: MetaTypes](
    scope: MT#ResourceNameScope, // This scope is to resolve both basedOn and any tables referenced within the soql
    canonicalName: CanonicalName, // This is the canonical name of this query; it is assumed to be unique across scopes
    basedOn: ResourceName,
    soql: String,
    parameters: Map[HoleName, MT#ColumnType],
    hiddenColumns: Set[ColumnName]
  ) extends SoQLUnparsedTableDescription[MT] {
    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): Query[MT2] =
      copy(
        scope = scopeMap(scope),
        parameters = parameters.asInstanceOf[Map[HoleName, MT2#ColumnType]] // SAFETY: ColumnType isn't changing
      )
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { parsed =>
        TableDescription.Query[MT](
          scope,
          canonicalName,
          basedOn,
          parsed,
          soql,
          parameters,
          hiddenColumns
        )
      }

    private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
      tableName: DatabaseTableName => types.DatabaseTableName[MT2],
      columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
    )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): Query[MT2] = {
      this.asInstanceOf[Query[MT2]] // SAFETY: We have no labels
    }
  }
  object Query {
    private[UnparsedTableDescription] def encode[MT <: MetaTypes](implicit encRNS: JsonEncode[MT#ResourceNameScope], encCT: JsonEncode[MT#ColumnType]) =
      AutomaticJsonEncodeBuilder[Query[MT]]
    private[UnparsedTableDescription] def decode[MT <: MetaTypes](implicit decRNS: JsonDecode[MT#ResourceNameScope], decCT: JsonDecode[MT#ColumnType]) =
      AutomaticJsonDecodeBuilder[Query[MT]]
  }

  case class TableFunction[MT <: MetaTypes](
    scope: MT#ResourceNameScope, // This scope is to resolve any tables referenced within the soql
    canonicalName: CanonicalName, // This is the canonical name of this UDF; it is assumed to be unique across scopes
    soql: String,
    parameters: OrderedMap[HoleName, MT#ColumnType],
    hiddenColumns: Set[ColumnName]
  ) extends SoQLUnparsedTableDescription[MT] {
    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): TableFunction[MT2] =
      copy(
        scope = scopeMap(scope),
        parameters = parameters.asInstanceOf[OrderedMap[HoleName, MT2#ColumnType]] // SAFETY: ColumnType isn't changing
      )

    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = true)).map { parsed =>
        TableDescription.TableFunction(
          scope,
          canonicalName,
          parsed,
          soql,
          parameters,
          hiddenColumns
        )
      }

    private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
      tableName: DatabaseTableName => types.DatabaseTableName[MT2],
      columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
    )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): TableFunction[MT2] = {
      this.asInstanceOf[TableFunction[MT2]] // SAFETY: We have no labels
    }
  }

  object TableFunction {
    private[UnparsedTableDescription] def encode[MT <: MetaTypes](implicit encRNS: JsonEncode[MT#ResourceNameScope], encCT: JsonEncode[MT#ColumnType]): JsonEncode[TableFunction[MT]] =
      new JsonEncode[TableFunction[MT]] with MetaTypeHelper[MT] {
        implicit val paramsEncode = OrderedMapHelper.jsonEncode[HoleName, CT]
        val encoder = AutomaticJsonEncodeBuilder[TableFunction[MT]]

        def encode(u: TableFunction[MT]): JValue = encoder.encode(u)
      }

    private[UnparsedTableDescription] def decode[MT <: MetaTypes](implicit decRNS: JsonDecode[MT#ResourceNameScope], decCT: JsonDecode[MT#ColumnType]): JsonDecode[TableFunction[MT]] =
      new JsonDecode[TableFunction[MT]] with MetaTypeHelper[MT] {
        implicit val paramsDecode = OrderedMapHelper.jsonDecode[HoleName, CT]
        val decoder = AutomaticJsonDecodeBuilder[TableFunction[MT]]

        def decode(v: JValue) = decoder.decode(v)
      }
  }
}

