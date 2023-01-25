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
        utd match {
          case UnparsedTableDescription.Dataset(name, canonicalName, schema, ordering, pk) =>
            rn -> TableDescription.Dataset(name, canonicalName, schema, ordering, pk)
          case other: UnparsedTableDescription.SoQLUnparsedTableDescription[ResourceNameScope, ColumnType] =>
            other.parse(params) match {
              case Right(ptd) => rn -> ptd
              case Left(e) => return Left(e)
            }
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
          case UnparsedTableDescription.Dataset(name, canonicalName, schema, ordering, pk) =>
            UnparsedTableDescription.Dataset(
              tableName(name),
              canonicalName,
              OrderedMap(schema.iterator.map { case (dcn, ne) =>
                columnName(name, dcn) -> ne
              }.toSeq : _*),
              ordering.map { case TableDescription.Ordering(dcn, ascending) =>
                TableDescription.Ordering(columnName(name, dcn), ascending)
              },
              pk.map(_.map(columnName(name, _)))
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

  private[analyzer2] def asMockTableFinder[MT <: MetaTypes](self: UnparsedTableMap[MT#RNS, MT#CT]): mocktablefinder.MockTableFinder[MT] = {
    new mocktablefinder.MockTableFinder[MT](
      self.underlying.iterator.flatMap { case (rns, resources) =>
        resources.iterator.map { case (rn, desc) =>
          val thing =
            desc match {
              case UnparsedTableDescription.Dataset(_name, _canonicalName, schema, ordering, pks) =>
                val base =
                  mocktablefinder.D(schema.valuesIterator.map { case TableDescription.DatasetColumnInfo(n, t, _) => n.name -> t }.toSeq : _*).
                    withHiddenColumns(schema.valuesIterator.filter(_.hidden).map(_.name.name).toSeq : _*)
                pks.foldLeft(
                  ordering.foldLeft(base) { (base, ordering) =>
                    base.withOrdering(schema(ordering.column).name.name, ordering.ascending)
                  }
                ) { (dataset, pk) => dataset.withPrimaryKey(pk.map(schema(_).name.name) : _*) }
              case UnparsedTableDescription.Query(scope, canonicalName, basedOn, soql, parameters, hiddenColumns) =>
                mocktablefinder.Q(scope, basedOn.name, soql, parameters.toSeq.map { case (hn, ct) => hn.name -> ct } : _*).
                  withCanonicalName(canonicalName.name).
                  withHiddenColumns(hiddenColumns.map(_.name).toSeq : _*)
              case UnparsedTableDescription.TableFunction(scope, canonicalName, soql, parameters, hiddenColumns) =>
                mocktablefinder.U(scope, soql, parameters.toSeq.map { case (hn, ct) => hn.name -> ct } : _*).
                  withCanonicalName(canonicalName.name).
                  withHiddenColumns(hiddenColumns.map(_.name).toSeq : _*)
            }
          (rns, rn.name) -> thing
        }
      }.toMap
    )
  }
}

