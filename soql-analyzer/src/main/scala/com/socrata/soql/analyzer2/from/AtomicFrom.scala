package com.socrata.soql.analyzer2.from

import scala.language.higherKinds
import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

trait AtomicFromImpl[+RNS, +CT, +CV] { this: AtomicFrom[RNS, CT, CV] =>
  type Self[+RNS, +CT, +CV] <: AtomicFrom[RNS, CT, CV]

  val resourceName: Option[ScopedResourceName[RNS]]
  val alias: Option[ResourceName]
  val label: TableLabel

  private[analyzer2] val scope: Scope[CT]

  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]) = {
    addToEnvironment(base.extend)
  }
  private[analyzer2] def addToEnvironment[CT2 >: CT](env: Environment[CT2]) = {
    env.addScope(alias, scope)
  }

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): Self[RNS, CT, CV]

  type ReduceResult[+RNS, +CT, +CV] = AtomicFrom[RNS, CT, CV]
  override def reduceMap[S, RNS2, CT2, CV2](
    base: AtomicFrom[RNS, CT, CV] => (S, AtomicFrom[RNS2, CT2, CV2]),
    combine: (S, JoinType, Boolean, From[RNS2, CT2, CV2], AtomicFrom[RNS, CT, CV], Expr[CT, CV]) => (S, Join[RNS2, CT2, CV2])
  ): (S, ReduceResult[RNS2, CT2, CV2]) =
    base(this)
}

trait OAtomicFromImpl { this: AtomicFrom.type =>
  implicit def serialize[RNS: Writable, CT: Writable, CV](implicit ev: Writable[Expr[CT, CV]]): Writable[AtomicFrom[RNS, CT, CV]] = new Writable[AtomicFrom[RNS, CT, CV]] {
    def writeTo(buffer: WriteBuffer, from: AtomicFrom[RNS, CT, CV]): Unit = {
      from match {
        case ft: FromTable[RNS, CT] =>
          buffer.write(0)
          buffer.write(ft)
        case fs: FromStatement[RNS, CT, CV] =>
          buffer.write(1)
          buffer.write(fs)
        case fsr: FromSingleRow[RNS] =>
          buffer.write(2)
          buffer.write(fsr)
      }
    }
  }

  implicit def deserialize[RNS: Readable, CT: Readable, CV](implicit ev: Readable[Expr[CT, CV]]): Readable[AtomicFrom[RNS, CT, CV]] = new Readable[AtomicFrom[RNS, CT, CV]] {
    def readFrom(buffer: ReadBuffer): AtomicFrom[RNS, CT, CV] = {
      buffer.read[Int]() match {
        case 0 => buffer.read[FromTable[RNS, CT]]()
        case 1 => buffer.read[FromStatement[RNS, CT, CV]]()
        case 2 => buffer.read[FromSingleRow[RNS]]()
        case other => fail("Unknown atomic from tag " + other)
      }
    }
  }
}
