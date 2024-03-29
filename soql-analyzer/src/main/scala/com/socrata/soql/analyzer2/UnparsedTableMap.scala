package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.analyzer2

class UnparsedTableMap[MT <: MetaTypes] private[analyzer2] (private val underlying: Map[MT#ResourceNameScope, Map[ResourceName, UnparsedTableDescription[MT]]]) extends TableMapLike[MT] {
  override def hashCode = underlying.hashCode
  override def equals(o: Any) =
    o match {
      case that: UnparsedTableMap[_] => this.underlying == that.underlying
      case _ => false
    }

  type Self[MT <: MetaTypes] = UnparsedTableMap[MT]

  private[analyzer2] def parse(params: AbstractParser.Parameters): Either[LexerParserException, TableMap[MT]] =
    Right(new TableMap(underlying.iterator.map { case (rns, m) =>
      rns -> m.iterator.map { case (rn, utd) =>
        utd match {
          case UnparsedTableDescription.Dataset(name, canonicalName, schema, ordering, pk) =>
            rn -> TableDescription.Dataset(name, canonicalName, schema, ordering, pk)
          case other: UnparsedTableDescription.SoQLUnparsedTableDescription[MT] =>
            other.parse(params) match {
              case Right(ptd) => rn -> ptd
              case Left(e) => return Left(e)
            }
        }
      }.toMap
    }.toMap))

  def rewriteDatabaseNames[MT2 <: MetaTypes](
    tableName: DatabaseTableName => types.DatabaseTableName[MT2],
    columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
  )(implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, MT2]): UnparsedTableMap[MT2] = {
    new UnparsedTableMap(underlying.iterator.map { case (rns, m) =>
      changesOnlyLabels.convertRNS(rns) -> m.iterator.map { case (rn, ptd) =>
        rn -> ptd.rewriteDatabaseNames(tableName, columnName)
      }.toMap
    }.toMap)
  }

  def allTableDescriptions =
    for {
      tablesForScope <- underlying.valuesIterator
      d@UnparsedTableDescription.Dataset(_, _, _, _, _) <- tablesForScope.valuesIterator
    } yield d

  def get(name: ScopedResourceName) = underlying.get(name.scope).flatMap(_.get(name.name))
}

object UnparsedTableMap {
  implicit def jsonEncode[MT <: MetaTypes](implicit encRNS: JsonEncode[MT#ResourceNameScope], encCT: JsonEncode[MT#ColumnType], encDTN: JsonEncode[MT#DatabaseTableNameImpl], encDCN: JsonEncode[MT#DatabaseColumnNameImpl]): JsonEncode[UnparsedTableMap[MT]] =
    new JsonEncode[UnparsedTableMap[MT]] {
      def encode(t: UnparsedTableMap[MT]) = JsonEncode.toJValue(t.underlying.toSeq)
    }

  implicit def jsonDecode[MT <: MetaTypes](implicit encRNS: JsonDecode[MT#ResourceNameScope], encCT: JsonDecode[MT#ColumnType], decDTN: JsonDecode[MT#DatabaseTableNameImpl], decDCN: JsonDecode[MT#DatabaseColumnNameImpl]): JsonDecode[UnparsedTableMap[MT]] =
    new JsonDecode[UnparsedTableMap[MT]] with MetaTypeHelper[MT] {
      def decode(v: JValue) =
        JsonDecode.fromJValue[Seq[(RNS, Map[ResourceName, UnparsedTableDescription[MT]])]](v).map { fields =>
          new UnparsedTableMap(fields.toMap)
        }
    }

  private[analyzer2] def asMockTableFinder[MT <: MetaTypes](self: UnparsedTableMap[MT])(implicit dtnIsString: String =:= MT#DatabaseTableNameImpl, dcnIsString: String =:= MT#DatabaseColumnNameImpl): mocktablefinder.MockTableFinder[MT] = {
    new mocktablefinder.MockTableFinder[MT](
      self.underlying.iterator.flatMap { case (rns, resources) =>
        resources.iterator.map { case (rn, desc) =>
          val thing =
            desc match {
              case UnparsedTableDescription.Dataset(_name, _canonicalName, schema, ordering, pks) =>
                val base =
                  mocktablefinder.D(schema.valuesIterator.map { case TableDescription.DatasetColumnInfo(n, t, _, _) => n.name -> t }.toSeq : _*).
                    withHiddenColumns(schema.valuesIterator.filter(_.hidden).map(_.name.name).toSeq : _*).
                    withOutputColumnHints(
                      schema.valuesIterator.collect {
                        case TableDescription.DatasetColumnInfo(name, _, _, Some(hint)) =>
                          name.name -> hint
                      }.toSeq : _*
                    )
                pks.foldLeft(
                  ordering.foldLeft(base) { (base, ordering) =>
                    base.withOrdering(schema(ordering.column).name.name, ordering.ascending)
                  }
                ) { (dataset, pk) => dataset.withPrimaryKey(pk.map(schema(_).name.name) : _*) }
              case UnparsedTableDescription.Query(scope, canonicalName, basedOn, soql, parameters, hiddenColumns, outputColumnHints) =>
                mocktablefinder.Q(scope, basedOn.name, soql, parameters.toSeq.map { case (hn, ct) => hn.name -> ct } : _*).
                  withCanonicalName(canonicalName.name).
                  withHiddenColumns(hiddenColumns.map(_.name).toSeq : _*).
                  withOutputColumnHints(outputColumnHints.map { case (k, v) => k.name -> v}.toSeq : _*)
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

