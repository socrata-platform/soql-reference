package com.socrata.soql.analyzer2

import scala.language.higherKinds

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, AutomaticJsonCodecBuilder}

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ResourceName, ColumnName}
import com.socrata.soql.parsing.AbstractParser

trait TableMapLike[MT <: MetaTypes] extends MetaTypeHelper[MT] with LabelHelper[MT] {
  type Self[MT <: MetaTypes] <: TableMapLike[MT]

  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): Self[MT]
}

class TableMap[MT <: MetaTypes] private[analyzer2] (private val underlying: Map[MT#ResourceNameScope, Map[ResourceName, TableDescription[MT]]]) extends TableMapLike[MT] {
  override def hashCode = underlying.hashCode
  override def equals(o: Any) =
    o match {
      case that: TableMap[_] => this.underlying == that.underlying
      case _ => false
    }

  type Self[MT <: MetaTypes] = TableMap[MT]
  type ScopedResourceName = com.socrata.soql.analyzer2.ScopedResourceName[RNS]

  def asUnparsedTableMap: UnparsedTableMap[MT] =
    new UnparsedTableMap(underlying.iterator.map { case (rns, m) =>
      rns -> m.iterator.map { case (rn, ptd) =>
        rn -> ptd.asUnparsedTableDescription
      }.toMap
    }.toMap)

  def contains(name: ScopedResourceName) =
    underlying.get(name.scope) match {
      case Some(resources) => resources.contains(name.name)
      case None => false
    }

  def get(name: ScopedResourceName) = underlying.get(name.scope).flatMap(_.get(name.name))

  def getOrElse(name: ScopedResourceName)(orElse: => TableDescription[MT]) = get(name).getOrElse(orElse)

  def size = underlying.valuesIterator.map(_.size).sum

  def +(kv: (ScopedResourceName, TableDescription[MT])): TableMap[MT] = {
    val (ScopedResourceName(rns, rn), desc) = kv
    underlying.get(rns) match {
      case Some(resources) => new TableMap(underlying + (rns -> (resources + (rn -> desc))))
      case None => new TableMap(underlying + (rns -> Map(rn -> desc)))
    }
  }

  def find(name: ScopedResourceName): TableDescription[MT] = {
    getOrElse(name) {
      throw new NoSuchElementException(s"TableMap: No such key: ${name.scope}:${name.name}")
    }
  }

  def descriptions = underlying.valuesIterator.flatMap(_.valuesIterator)

  private[analyzer2] def rewriteScopes(topLevel: RNS): (TableMap[Intified[MT]], Map[Int, RNS], Map[RNS, Int]) = {
    val oldToNew = (underlying.keysIterator ++ Iterator.single(topLevel)).zipWithIndex.toMap
    val newToOld = oldToNew.iterator.map(_.swap).toMap
    val newMap =
      underlying.iterator.map { case (rns, resources) =>
        oldToNew(rns) -> resources.iterator.map { case (k, v) => k -> v.rewriteScopes[Intified[MT]](oldToNew) }.toMap
      }.toMap

    (new TableMap[Intified[MT]](newMap), newToOld, oldToNew)
  }

  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): TableMap[MT] = {
    new TableMap(underlying.iterator.map { case (rns, m) =>
      rns -> m.iterator.map { case (rn, ptd) =>
        val newptd = ptd match {
          case TableDescription.Dataset(name, canonicalName, schema, ordering, pk) =>
            TableDescription.Dataset(
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

  override def toString = "TableMap(" + underlying + ")"
}

object TableMap {
  def empty[MT <: MetaTypes] = new TableMap[MT](Map.empty)

  private [analyzer2] def jsonEncode[MT <: MetaTypes](implicit encUnparsed: JsonEncode[UnparsedTableMap[MT]]): JsonEncode[TableMap[MT]] =
    new JsonEncode[TableMap[MT]] {
      def encode(t: TableMap[MT]) = JsonEncode.toJValue(t.asUnparsedTableMap)
    }

  private [analyzer2] def jsonDecode[MT <: MetaTypes](parameters: AbstractParser.Parameters)(implicit decRNS: JsonDecode[MT#RNS], decUnparsed: JsonDecode[UnparsedTableDescription[MT]]): JsonDecode[TableMap[MT]] =
    new JsonDecode[TableMap[MT]] with MetaTypeHelper[MT] {
      // this can't just go via UnparsedTableMap for error-tracking
      // reasons.  We have to use the TableDescription
      // JsonDecode directly.
      private implicit val ptdDecode = TableDescription.jsonDecode[MT](parameters)
      def decode(v: JValue) = JsonDecode.fromJValue[Seq[(RNS, Map[ResourceName, TableDescription[MT]])]](v).map { fields =>
        new TableMap(fields.toMap)
      }
    }
}
