package com.socrata.soql.analyzer2

import scala.collection.compat.immutable.LazyList

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, NullForNone}

import com.socrata.soql.environment.{ColumnName, ResourceName}

class LabelMap[MT <: MetaTypes] private[analyzer2] (
  val tableMap: Map[types.TableLabel[MT], types.LabelMap.TableReference[MT]],
  val columnMap: Map[(types.TableLabel[MT], types.ColumnLabel[MT]), (types.LabelMap.TableReference[MT], ColumnName)]
) {
  override def toString = s"LabelMap($tableMap, $columnMap)"
}

object LabelMap {
  @NullForNone
  case class TableReference[+RNS](
    resourceName: Option[ScopedResourceName[RNS]],
    alias: Option[ResourceName]
  )
  object TableReference {
    implicit def encode[RNS: JsonEncode] = AutomaticJsonEncodeBuilder[TableReference[RNS]]
  }

  implicit def encode[MT <: MetaTypes](implicit rnsEncode: JsonEncode[types.ResourceNameScope[MT]], tlEncode: JsonEncode[types.TableLabel[MT]], clEncode: JsonEncode[types.ColumnLabel[MT]]) = new JsonEncode[LabelMap[MT]] {
    def encode(x: LabelMap[MT]) = {
      val tmSeq = x.tableMap.to(LazyList).map { case (label, table) =>
        json"""{ label: $label, table: $table }"""
      }
      val cmSeq = x.columnMap.to(LazyList).map { case ((tLabel, cLabel), (qual, col)) =>
        json"""{ tableLabel: $tLabel, columnLabel: $cLabel, qualifier: $qual, column: $col }"""
      }
      json"""{ tableMap: $tmSeq, columnMap: $cmSeq }"""
    }
  }
}

private[analyzer2] class LabelMapState[MT <: MetaTypes] extends LabelUniverse[MT] {
  val tableMap = Map.newBuilder[TableLabel, LabelMap.TableReference[RNS]]
  val columnMap = Map.newBuilder[(TableLabel, ColumnLabel), (LabelMap.TableReference[RNS], ColumnName)]

  def build() = new LabelMap(tableMap.result(), columnMap.result())
}
