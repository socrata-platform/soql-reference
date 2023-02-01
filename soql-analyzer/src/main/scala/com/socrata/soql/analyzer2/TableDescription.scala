package com.socrata.soql.analyzer2

import scala.collection.compat._

import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder}

import com.socrata.soql.ast
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ResourceName, ColumnName, HoleName}
import com.socrata.soql.BinaryTree
import com.socrata.soql.analyzer2

trait TableDescriptionLike {
  val canonicalName: CanonicalName // This is the canonical name of this query or table; it is assumed to be unique across scopes
  val hiddenColumns: Set[ColumnName]
}

sealed trait TableDescription[MT <: MetaTypes] extends TableDescriptionLike with LabelUniverse[MT] {
  private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): TableDescription[MT2]
  private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
    tableName: DatabaseTableName => analyzer2.DatabaseTableName[MT2#DatabaseTableNameImpl],
    columnName: (DatabaseTableName, DatabaseColumnName) => analyzer2.DatabaseColumnName[MT2#DatabaseColumnNameImpl]
  )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): TableDescription[MT2]

  def asUnparsedTableDescription: UnparsedTableDescription[MT]
}

object TableDescription {
  private[analyzer2] def jsonEncode[MT <: MetaTypes](implicit enc: JsonEncode[UnparsedTableDescription[MT]]) =
    new JsonEncode[TableDescription[MT]] {
      def encode(x: TableDescription[MT]) =
        JsonEncode.toJValue(x.asUnparsedTableDescription)
    }

  private[analyzer2] def jsonDecode[MT <: MetaTypes](parserParameters: AbstractParser.Parameters)(implicit dec: JsonDecode[UnparsedTableDescription[MT]]) =
    new JsonDecode[TableDescription[MT]] {
      def decode(x: JValue) =
        JsonDecode.fromJValue[UnparsedTableDescription[MT]](x).flatMap {
          case c: UnparsedTableDescription.SoQLUnparsedTableDescription[MT] =>
            c.parse(parserParameters).left.map { _ =>
              DecodeError.InvalidValue(JString(c.soql)).prefix("soql")
            }
          case UnparsedTableDescription.Dataset(name, canonicalName, schema, ordering, primaryKey) =>
            Right(Dataset[MT](name, canonicalName, schema, ordering, primaryKey))
        }
    }

  case class Ordering[MT <: MetaTypes](column: DatabaseColumnName[MT#DatabaseColumnNameImpl], ascending: Boolean)
  object Ordering {
    private[analyzer2] implicit def encode[MT <: MetaTypes](implicit colEnc: JsonEncode[MT#DatabaseColumnNameImpl]) = AutomaticJsonEncodeBuilder[Ordering[MT]]
    private[analyzer2] implicit def decode[MT <: MetaTypes](implicit colDec: JsonDecode[MT#DatabaseColumnNameImpl]) = AutomaticJsonDecodeBuilder[Ordering[MT]]
  }

  case class DatasetColumnInfo[+ColumnType](name: ColumnName, typ: ColumnType, hidden: Boolean)
  object DatasetColumnInfo {
    private[analyzer2] implicit def encode[CT: JsonEncode] = AutomaticJsonEncodeBuilder[DatasetColumnInfo[CT]]
    private[analyzer2] implicit def decode[CT: JsonDecode] = AutomaticJsonDecodeBuilder[DatasetColumnInfo[CT]]
  }

  case class Dataset[MT <: MetaTypes](
    name: DatabaseTableName[MT#DatabaseTableNameImpl],
    canonicalName: CanonicalName,
    columns: OrderedMap[DatabaseColumnName[MT#DatabaseColumnNameImpl], DatasetColumnInfo[MT#ColumnType]],
    ordering: Seq[Ordering[MT]],
    primaryKeys: Seq[Seq[DatabaseColumnName[MT#DatabaseColumnNameImpl]]]
  ) extends TableDescription[MT] {
    for(o <- ordering) {
      require(columns.contains(o.column), "Ordering not in dataset")
    }
    for {
      pk <- primaryKeys
      col <- pk
    } {
      require(columns.contains(col), "Primary key not in dataset")
    }

    val hiddenColumns = columns.values.flatMap { case DatasetColumnInfo(name, _, hidden) =>
      if(hidden) Some(name) else None
    }.to(Set)

    val schema = OrderedMap() ++ columns.iterator.map {
      case (dcn, DatasetColumnInfo(name, typ, hidden)) => dcn -> NameEntry(name, typ)
    }

    require(ordering.forall { o => columns.contains(o.column) })

    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): TableDescription[MT2] =
      this.asInstanceOf[TableDescription[MT2]] // SAFETY: We only care about the NameImpls and ColumnType, neither of which are changing

    private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
      tableName: DatabaseTableName => analyzer2.DatabaseTableName[MT2#DatabaseTableNameImpl],
      columnName: (DatabaseTableName, DatabaseColumnName) => analyzer2.DatabaseColumnName[MT2#DatabaseColumnNameImpl]
    )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): Dataset[MT2] = {
      Dataset[MT2](
        tableName(name),
        canonicalName,
        OrderedMap(
          columns.iterator.map { case (dcn, ne) =>
            columnName(name, dcn) -> ev.convertCTOnly(ne)
          }.toSeq : _*
        ),
        ordering.map { case TableDescription.Ordering(dcn, ascending) =>
          TableDescription.Ordering(columnName(name, dcn), ascending)
        },
        primaryKeys.map(_.map(columnName(name, _)))
      )
    }

    def asUnparsedTableDescription =
      UnparsedTableDescription.Dataset(name, canonicalName, columns, ordering, primaryKeys)
  }

  case class Query[MT <: MetaTypes](
    scope: MT#ResourceNameScope, // This scope is to resolve both basedOn and any tables referenced within the soql
    canonicalName: CanonicalName,
    basedOn: ResourceName,
    parsed: BinaryTree[ast.Select],
    unparsed: String,
    parameters: Map[HoleName, MT#ColumnType],
    hiddenColumns: Set[ColumnName]
  ) extends TableDescription[MT] {
    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): Query[MT2] =
      copy(
        scope = scopeMap(scope),
        parameters = parameters.asInstanceOf // SAFETY: ColumnType isn't changing
      )

    private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
      tableName: DatabaseTableName => analyzer2.DatabaseTableName[MT2#DatabaseTableNameImpl],
      columnName: (DatabaseTableName, DatabaseColumnName) => analyzer2.DatabaseColumnName[MT2#DatabaseColumnNameImpl]
    )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): Query[MT2] =
      this.asInstanceOf[Query[MT2]] // SAFETY: ColumnType isn't changing

    def asUnparsedTableDescription =
      UnparsedTableDescription.Query(scope, canonicalName, basedOn, unparsed, parameters, hiddenColumns)
  }

  case class TableFunction[MT <: MetaTypes](
    scope: MT#ResourceNameScope, // This scope is to resolve any tables referenced within the soql
    canonicalName: CanonicalName,
    parsed: BinaryTree[ast.Select],
    unparsed: String,
    parameters: OrderedMap[HoleName, MT#ColumnType],
    hiddenColumns: Set[ColumnName]
  ) extends TableDescription[MT] {
    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): TableFunction[MT2] =
      copy(
        scope = scopeMap(scope),
        parameters = parameters.asInstanceOf // SAFETY: ColumnType isn't changing
      )

    private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
      tableName: DatabaseTableName => analyzer2.DatabaseTableName[MT2#DatabaseTableNameImpl],
      columnName: (DatabaseTableName, DatabaseColumnName) => analyzer2.DatabaseColumnName[MT2#DatabaseColumnNameImpl]
    )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): TableFunction[MT2] =
      this.asInstanceOf[TableFunction[MT2]] // SAFETY: ColumnType isn't changing

    def asUnparsedTableDescription =
      UnparsedTableDescription.TableFunction(scope, canonicalName, unparsed, parameters, hiddenColumns)
  }
}

