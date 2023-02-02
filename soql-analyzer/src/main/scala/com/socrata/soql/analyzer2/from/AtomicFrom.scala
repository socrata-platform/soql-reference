package com.socrata.soql.analyzer2.from

import scala.language.higherKinds
import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction

trait AtomicFromImpl[MT <: MetaTypes] { this: AtomicFrom[MT] =>
  type Self[MT <: MetaTypes] <: AtomicFrom[MT]

  val resourceName: Option[ScopedResourceName[RNS]]
  val alias: Option[ResourceName]
  val label: AutoTableLabel

  private[analyzer2] val scope: Scope[MT]

  private[analyzer2] def extendEnvironment(base: Environment[MT]) = {
    addToEnvironment(base.extend)
  }
  private[analyzer2] def addToEnvironment(env: Environment[MT]) = {
    env.addScope(alias, scope)
  }

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): Self[MT]

  type ReduceResult[MT <: MetaTypes] = AtomicFrom[MT]
  override def reduceMap[S, MT2 <: MetaTypes](
    base: AtomicFrom[MT] => (S, AtomicFrom[MT2]),
    combine: (S, JoinType, Boolean, From[MT2], AtomicFrom[MT], Expr[MT]) => (S, Join[MT2])
  ): (S, ReduceResult[MT2]) =
    base(this)
}

trait OAtomicFromImpl { this: AtomicFrom.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[AtomicFrom[MT]] = new Writable[AtomicFrom[MT]] {
    def writeTo(buffer: WriteBuffer, from: AtomicFrom[MT]): Unit = {
      from match {
        case ft: FromTable[MT] =>
          buffer.write(0)
          buffer.write(ft)
        case fs: FromStatement[MT] =>
          buffer.write(1)
          buffer.write(fs)
        case fsr: FromSingleRow[MT] =>
          buffer.write(2)
          buffer.write(fsr)
      }
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[AtomicFrom[MT]] = new Readable[AtomicFrom[MT]] {
    def readFrom(buffer: ReadBuffer): AtomicFrom[MT] = {
      buffer.read[Int]() match {
        case 0 => buffer.read[FromTable[MT]]()
        case 1 => buffer.read[FromStatement[MT]]()
        case 2 => buffer.read[FromSingleRow[MT]]()
        case other => fail("Unknown atomic from tag " + other)
      }
    }
  }
}
