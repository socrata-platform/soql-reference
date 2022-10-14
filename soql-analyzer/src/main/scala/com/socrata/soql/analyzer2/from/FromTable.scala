package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

trait FromTableImpl[+RNS, +CT] { this: FromTable[RNS, CT] =>
  type Self[+RNS, +CT, +CV] = FromTable[RNS, CT]
  def asSelf = this

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      tableName = state.convert(this.tableName),
      columns = OrderedMap() ++ columns.iterator.map { case (n, ne) => state.convert(this.tableName, n) -> ne }
    )

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  // A table has its columns whether or not they're selected, so this is just "this"
  private[analyzer2] def doRemoveUnusedColumns(used: Map[TableLabel, Set[ColumnLabel]]) = this

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case FromTable(thatTableName, thatAlias, thatLabel, thatColumns) =>
        this.tableName == thatTableName &&
          // don't care about aliases
          state.tryAssociate(this.label, thatLabel) &&
          this.columns.size == thatColumns.size &&
          this.columns.iterator.zip(thatColumns.iterator).forall { case ((thisColName, thisEntry), (thatColName, thatEntry)) =>
            thisColName == thatColName &&
              thisEntry.typ == thatEntry.typ
            // don't care about the entry's name
          }
      case _ =>
        false
    }

  private[analyzer2] def realTables = Map(label -> tableName)

  private[analyzer2] def reAlias[RNS2](newAlias: Option[(RNS2, ResourceName)]): FromTable[RNS2, CT] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, Nothing] =
    copy(alias = f(alias))

  def useSelectListReferences: this.type = this
}

trait OFromTableImpl { this: FromTable.type =>
  implicit def serialize[RNS: Writable, CT: Writable]: Writable[FromTable[RNS, CT]] = new Writable[FromTable[RNS, CT]] {
    def writeTo(buffer: WriteBuffer, from: FromTable[RNS, CT]): Unit = {
      buffer.write(from.tableName)
      buffer.write(from.alias)
      buffer.write(from.label)
      buffer.write(from.columns)
    }
  }

  implicit def deserialize[RNS: Readable, CT: Readable]: Readable[FromTable[RNS, CT]] = new Readable[FromTable[RNS, CT]] {
    def readFrom(buffer: ReadBuffer): FromTable[RNS, CT] = {
      FromTable(
        tableName = buffer.read[DatabaseTableName](),
        alias = buffer.read[Option[(RNS, ResourceName)]](),
        label = buffer.read[AutoTableLabel](),
        columns = buffer.read[OrderedMap[DatabaseColumnName, NameEntry[CT]]]()
      )
    }
  }
}
