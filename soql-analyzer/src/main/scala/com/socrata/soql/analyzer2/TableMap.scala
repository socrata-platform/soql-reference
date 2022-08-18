package com.socrata.soql.analyzer2

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}

import com.socrata.soql.environment.ResourceName

class TableMap[ResourceNameScope, +ColumnType](private val underlying: Map[ResourceNameScope, Map[ResourceName, ParsedTableDescription[ResourceNameScope, ColumnType]]]) extends AnyVal {
  type ScopedResourceName = (ResourceNameScope, ResourceName)

  def contains(name: ScopedResourceName) =
    underlying.get(name._1) match {
      case Some(resources) => resources.contains(name._2)
      case None => false
    }

  def get(name: ScopedResourceName) = underlying.get(name._1).flatMap(_.get(name._2))

  def getOrElse[CT2 >: ColumnType](name: ScopedResourceName)(orElse: => ParsedTableDescription[ResourceNameScope, CT2]) = get(name).getOrElse(orElse)

  def size = underlying.valuesIterator.map(_.size).sum

  def +[CT2 >: ColumnType](kv: (ScopedResourceName, ParsedTableDescription[ResourceNameScope, CT2])): TableMap[ResourceNameScope, CT2] = {
    val ((rns, rn), desc) = kv
    underlying.get(rns) match {
      case Some(resources) => new TableMap(underlying + (rns -> (resources + (rn -> desc))))
      case None => new TableMap(underlying + (rns -> Map(rn -> desc)))
    }
  }

  def find(scope: ResourceNameScope, name: ResourceName): ParsedTableDescription[ResourceNameScope, ColumnType] = {
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

  override def toString = "TableMap(" + underlying + ")"
}
object TableMap {
  def empty[ResourceNameScope, ColumnType] = new TableMap[ResourceNameScope, ColumnType](Map.empty)

  implicit def jEncode[RNS: JsonEncode, CT: JsonEncode]: JsonEncode[TableMap[RNS, CT]] =
    new JsonEncode[TableMap[RNS, CT]] {
      def encode(t: TableMap[RNS, CT]) = JsonEncode.toJValue(t.underlying.toSeq)
    }

  implicit def jDecode[RNS: JsonDecode, CT: JsonDecode]: JsonDecode[TableMap[RNS, CT]] =
    new JsonDecode[TableMap[RNS, CT]] {
      def decode(v: JValue) = JsonDecode.fromJValue[Seq[(RNS, Map[ResourceName, ParsedTableDescription[RNS, CT]])]](v).map { fields =>
        new TableMap(fields.toMap)
      }
    }
}
