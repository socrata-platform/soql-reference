package com.socrata.soql.analyzer2

import scala.language.higherKinds

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, AutomaticJsonCodecBuilder}

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ResourceName, ColumnName}
import com.socrata.soql.parsing.AbstractParser

trait TableMapLike[ResourceNameScope, +ColumnType] extends Any {
  type Self[RNS, +CT]

  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): Self[ResourceNameScope, ColumnType]
}

class TableMap[ResourceNameScope, +ColumnType] private[analyzer2] (private val underlying: Map[ResourceNameScope, Map[ResourceName, TableDescription[ResourceNameScope, ColumnType]]]) extends AnyVal with TableMapLike[ResourceNameScope, ColumnType] {
  type Self[RNS, +CT] = TableMap[RNS, CT]
  type ScopedResourceName = (ResourceNameScope, ResourceName)

  def asUnparsedTableMap: UnparsedTableMap[ResourceNameScope, ColumnType] =
    new UnparsedTableMap(underlying.iterator.map { case (rns, m) =>
      rns -> m.iterator.map { case (rn, ptd) =>
        rn -> ptd.asUnparsedTableDescription
      }.toMap
    }.toMap)

  def contains(name: ScopedResourceName) =
    underlying.get(name._1) match {
      case Some(resources) => resources.contains(name._2)
      case None => false
    }

  def get(name: ScopedResourceName) = underlying.get(name._1).flatMap(_.get(name._2))

  def getOrElse[CT2 >: ColumnType](name: ScopedResourceName)(orElse: => TableDescription[ResourceNameScope, CT2]) = get(name).getOrElse(orElse)

  def size = underlying.valuesIterator.map(_.size).sum

  def +[CT2 >: ColumnType](kv: (ScopedResourceName, TableDescription[ResourceNameScope, CT2])): TableMap[ResourceNameScope, CT2] = {
    val ((rns, rn), desc) = kv
    underlying.get(rns) match {
      case Some(resources) => new TableMap(underlying + (rns -> (resources + (rn -> desc))))
      case None => new TableMap(underlying + (rns -> Map(rn -> desc)))
    }
  }

  def find(scope: ResourceNameScope, name: ResourceName): TableDescription[ResourceNameScope, ColumnType] = {
    getOrElse((scope, name)) {
      throw new NoSuchElementException(s"TableMap: No such key: $scope:$name")
    }
  }

  def descriptions = underlying.valuesIterator.flatMap(_.valuesIterator)

  private[analyzer2] def rewriteScopes(topLevel: ResourceNameScope): (TableMap[Int, ColumnType], Map[Int, ResourceNameScope], Map[ResourceNameScope, Int]) = {
    val oldToNew = (underlying.keysIterator ++ Iterator.single(topLevel)).zipWithIndex.toMap
    val newToOld = oldToNew.iterator.map(_.swap).toMap
    val newMap =
      underlying.iterator.map { case (rns, resources) =>
        oldToNew(rns) -> resources.iterator.map { case (k, v) => k -> v.rewriteScopes(oldToNew) }.toMap
      }.toMap

    (new TableMap(newMap), newToOld, oldToNew)
  }

  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): TableMap[ResourceNameScope, ColumnType] = {
    new TableMap(underlying.iterator.map { case (rns, m) =>
      rns -> m.iterator.map { case (rn, ptd) =>
        val newptd = ptd match {
          case TableDescription.Dataset(name, canonicalName, schema) =>
            TableDescription.Dataset(
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

  override def toString = "TableMap(" + underlying + ")"
}

object TableMap {
  def empty[ResourceNameScope, ColumnType] = new TableMap[ResourceNameScope, ColumnType](Map.empty)

  private [analyzer2] def jsonEncode[RNS: JsonEncode, CT: JsonEncode]: JsonEncode[TableMap[RNS, CT]] =
    new JsonEncode[TableMap[RNS, CT]] {
      def encode(t: TableMap[RNS, CT]) = JsonEncode.toJValue(t.asUnparsedTableMap)
    }

  private [analyzer2] def jsonDecode[RNS: JsonDecode, CT: JsonDecode](parameters: AbstractParser.Parameters): JsonDecode[TableMap[RNS, CT]] =
    new JsonDecode[TableMap[RNS, CT]] {
      // this can't just go via UnparsedTableMap for error-tracking
      // reasons.  We have to use the TableDescription
      // JsonDecode directly.
      private implicit val ptdDecode = TableDescription.jsonDecode[RNS, CT](parameters)
      def decode(v: JValue) = JsonDecode.fromJValue[Seq[(RNS, Map[ResourceName, TableDescription[RNS, CT]])]](v).map { fields =>
        new TableMap(fields.toMap)
      }
    }
}
