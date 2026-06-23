package com.socrata.soql.analyzer2

import scala.collection.compat._

import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder}

import com.socrata.soql.ast
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ResourceName, ScopedResourceName, ColumnName, HoleName}
import com.socrata.soql.BinaryTree
import com.socrata.soql.analyzer2

trait TableDescriptionLike {
  val canonicalName: CanonicalName // This is the canonical name of this query or table; it is assumed to be unique across scopes
  val hiddenColumns: Set[ColumnName]
}

object TableDescriptionLike {
  trait Dataset[MT <: MetaTypes] {
    val name: types.DatabaseTableName[MT]
    val canonicalName: CanonicalName
    val columns: OrderedMap[types.DatabaseColumnName[MT], TableDescription.DatasetColumnInfo[MT#ColumnType]]
    val ordering: Seq[TableDescription.Ordering[MT]]
    val primaryKeys: Seq[Seq[types.DatabaseColumnName[MT]]]
  }
}

sealed trait TableDescription[MT <: MetaTypes] extends TableDescriptionLike with LabelUniverse[MT] {
  private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): TableDescription[MT2]
  private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
    tableName: DatabaseTableName => types.DatabaseTableName[MT2],
    columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
  )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): TableDescription[MT2]

  def directlyReferencedTables: Set[types.ScopedResourceName[MT]]

  def asUnparsedTableDescription: UnparsedTableDescription[MT]

  def wrappingQuery: Option[TableDescription.WrappingQuery[MT]]

  protected def directlyReferencedTablesInWrapper: Set[ScopedResourceName] =
    wrappingQuery.fold(Set.empty[ScopedResourceName]) { wrapper =>
      Util.walkParsed[MT](Set.empty[ScopedResourceName], wrapper.scope, wrapper.parsed)
    }
}

object TableDescription {
  case class WrappingQuery[MT <: MetaTypes](
    scope: MT#ResourceNameScope,
    parsed: BinaryTree[ast.Select],
    unparsed: String
  ) extends LabelUniverse[MT] {
    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): WrappingQuery[MT2] =
      WrappingQuery(
        scopeMap(scope),
        parsed,
        unparsed
      )
    def rewriteDatabaseNames[MT2 <: MetaTypes](implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]) =
      this.asInstanceOf[WrappingQuery[MT2]] // SAFETY: no labels in here

    def asUnparsedWrappingQuery = UnparsedTableDescription.WrappingQuery[MT](scope, unparsed)
  }

  private[analyzer2] def jsonEncode[MT <: MetaTypes](implicit enc: JsonEncode[UnparsedTableDescription[MT]]) =
    new JsonEncode[TableDescription[MT]] {
      def encode(x: TableDescription[MT]) =
        JsonEncode.toJValue(x.asUnparsedTableDescription)
    }

  private[analyzer2] def jsonDecode[MT <: MetaTypes](parserParameters: AbstractParser.Parameters)(implicit dec: JsonDecode[UnparsedTableDescription[MT]]) =
    new JsonDecode[TableDescription[MT]] {
      def decode(x: JValue) =
        JsonDecode.fromJValue[UnparsedTableDescription[MT]](x).flatMap { unparsed =>
          unparsed.parse(parserParameters).left.map { case (err, fields, text) =>
            fields.reverse.foldLeft[DecodeError](DecodeError.InvalidValue(JString(text)))(_.prefix(_))
          }
        }
    }

  case class Ordering[MT <: MetaTypes](column: types.DatabaseColumnName[MT], ascending: Boolean, nullLast: Boolean)
  object Ordering {
    private[analyzer2] implicit def encode[MT <: MetaTypes](implicit colEnc: JsonEncode[MT#DatabaseColumnNameImpl]) = AutomaticJsonEncodeBuilder[Ordering[MT]]
    private[analyzer2] implicit def decode[MT <: MetaTypes](implicit colDec: JsonDecode[MT#DatabaseColumnNameImpl]) = AutomaticJsonDecodeBuilder[Ordering[MT]]
  }

  case class DatasetColumnInfo[+ColumnType](name: ColumnName, typ: ColumnType, hidden: Boolean, hint: Option[JValue])
  object DatasetColumnInfo {
    private[analyzer2] implicit def encode[CT: JsonEncode] = AutomaticJsonEncodeBuilder[DatasetColumnInfo[CT]]
    private[analyzer2] implicit def decode[CT: JsonDecode] = AutomaticJsonDecodeBuilder[DatasetColumnInfo[CT]]
  }

  case class Dataset[MT <: MetaTypes](
    name: types.DatabaseTableName[MT],
    canonicalName: CanonicalName,
    columns: OrderedMap[types.DatabaseColumnName[MT], DatasetColumnInfo[MT#ColumnType]],
    ordering: Seq[Ordering[MT]],
    primaryKeys: Seq[Seq[types.DatabaseColumnName[MT]]],
    wrappingQuery: Option[WrappingQuery[MT]]
  ) extends TableDescription[MT] with TableDescriptionLike.Dataset[MT] {
    for(o <- ordering) {
      require(columns.contains(o.column), "Ordering not in dataset")
    }
    for {
      pk <- primaryKeys
      col <- pk
    } {
      require(columns.contains(col), "Primary key not in dataset")
    }

    val hiddenColumns = columns.values.flatMap { case DatasetColumnInfo(name, _, hidden, _) =>
      if(hidden) Some(name) else None
    }.to(Set)

    val schema = OrderedMap() ++ columns.iterator.map {
      case (dcn, DatasetColumnInfo(name, typ, hidden, hint)) => dcn -> FromTable.ColumnInfo[MT](name, typ, hint)
    }

    require(ordering.forall { o => columns.contains(o.column) })

    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): TableDescription[MT2] =
      Dataset[MT2](
        ev.convertDTN(name),
        canonicalName,
        // SAFETY: None of these next three things contain Scopes
        columns.asInstanceOf[OrderedMap[types.DatabaseColumnName[MT2], DatasetColumnInfo[MT2#ColumnType]]],
        ordering.asInstanceOf[Seq[Ordering[MT2]]],
        primaryKeys.asInstanceOf[Seq[Seq[types.DatabaseColumnName[MT2]]]],
        wrappingQuery.map(_.rewriteScopes[MT2](scopeMap))
      )

    private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
      tableName: DatabaseTableName => types.DatabaseTableName[MT2],
      columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
    )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): Dataset[MT2] = {
      Dataset[MT2](
        tableName(name),
        canonicalName,
        OrderedMap(
          columns.iterator.map { case (dcn, ne) =>
            columnName(name, dcn) -> ev.convertCTOnly(ne)
          }.toSeq : _*
        ),
        ordering.map { case TableDescription.Ordering(dcn, ascending, nullLast) =>
          TableDescription.Ordering(columnName(name, dcn), ascending, nullLast)
        },
        primaryKeys.map(_.map(columnName(name, _))),
        wrappingQuery.map(_.rewriteDatabaseNames[MT2])
      )
    }

    def asUnparsedTableDescription =
      UnparsedTableDescription.Dataset(name, canonicalName, columns, ordering, primaryKeys, wrappingQuery.map(_.asUnparsedWrappingQuery))

    def directlyReferencedTables: Set[types.ScopedResourceName[MT]] =
      directlyReferencedTablesInWrapper
  }

  case class Query[MT <: MetaTypes](
    scope: MT#ResourceNameScope, // This scope is to resolve both basedOn and any tables referenced within the soql
    canonicalName: CanonicalName,
    basedOn: ResourceName,
    parsed: BinaryTree[ast.Select],
    unparsed: String,
    parameters: Map[HoleName, MT#ColumnType],
    hiddenColumns: Set[ColumnName],
    outputColumnHints: Map[ColumnName, JValue],
    wrappingQuery: Option[WrappingQuery[MT]]
  ) extends TableDescription[MT] {
    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): Query[MT2] =
      copy(
        scope = scopeMap(scope),
        parameters = parameters.asInstanceOf[Map[HoleName, MT2#ColumnType]], // SAFETY: ColumnType isn't changing
        wrappingQuery = wrappingQuery.map(_.rewriteScopes[MT2](scopeMap))
      )

    private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
      tableName: DatabaseTableName => types.DatabaseTableName[MT2],
      columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
    )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): Query[MT2] =
      this.asInstanceOf[Query[MT2]] // SAFETY: ColumnType isn't changing

    def asUnparsedTableDescription =
      UnparsedTableDescription.Query(scope, canonicalName, basedOn, unparsed, parameters, hiddenColumns, outputColumnHints, wrappingQuery.map(_.asUnparsedWrappingQuery))

    def directlyReferencedTables: Set[types.ScopedResourceName[MT]] =
      Util.walkParsed[MT](Set(ScopedResourceName(scope, basedOn)), scope, parsed) ++
        directlyReferencedTablesInWrapper
  }

  case class TableFunction[MT <: MetaTypes](
    scope: MT#ResourceNameScope, // This scope is to resolve any tables referenced within the soql
    canonicalName: CanonicalName,
    parsed: BinaryTree[ast.Select],
    unparsed: String,
    parameters: OrderedMap[HoleName, MT#ColumnType],
    hiddenColumns: Set[ColumnName],
    wrappingQuery: Option[WrappingQuery[MT]],
  ) extends TableDescription[MT] {
    private[analyzer2] def rewriteScopes[MT2 <: MetaTypes](scopeMap: Map[RNS, MT2#ResourceNameScope])(implicit ev: MetaTypes.ChangesOnlyRNS[MT, MT2]): TableFunction[MT2] =
      copy(
        scope = scopeMap(scope),
        parameters = parameters.asInstanceOf[OrderedMap[HoleName, MT2#ColumnType]], // SAFETY: ColumnType isn't changing
        wrappingQuery = wrappingQuery.map(_.rewriteScopes[MT2](scopeMap))
      )

    private[analyzer2] def rewriteDatabaseNames[MT2 <: MetaTypes](
      tableName: DatabaseTableName => types.DatabaseTableName[MT2],
      columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
    )(implicit ev: MetaTypes.ChangesOnlyLabels[MT, MT2]): TableFunction[MT2] =
      this.asInstanceOf[TableFunction[MT2]] // SAFETY: ColumnType isn't changing

    def asUnparsedTableDescription =
      UnparsedTableDescription.TableFunction(scope, canonicalName, unparsed, parameters, hiddenColumns, wrappingQuery.map(_.asUnparsedWrappingQuery))

    def directlyReferencedTables: Set[types.ScopedResourceName[MT]] =
      Util.walkParsed[MT](Set.empty[types.ScopedResourceName[MT]], scope, parsed) ++
        directlyReferencedTablesInWrapper
  }

}

