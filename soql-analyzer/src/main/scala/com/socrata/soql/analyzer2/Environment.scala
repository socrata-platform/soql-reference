package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, JsonKey}

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName}
import com.socrata.soql.serialize.{Readable, ReadBuffer, Writable, WriteBuffer}

case class LabelEntry[MT <: MetaTypes](label: types.ColumnLabel[MT], @JsonKey("type") typ: types.ColumnType[MT])
object LabelEntry {
  implicit def jEncode[MT <: MetaTypes](implicit encLabel: JsonEncode[types.ColumnLabel[MT]], encCT: JsonEncode[types.ColumnType[MT]]) = AutomaticJsonEncodeBuilder[LabelEntry[MT]]

  implicit def jDecode[MT <: MetaTypes](implicit decLabel: JsonDecode[types.ColumnLabel[MT]], decCT: JsonDecode[types.ColumnType[MT]]) = AutomaticJsonDecodeBuilder[LabelEntry[MT]]

  implicit def serialize[MT <: MetaTypes](implicit encLabel: Writable[types.ColumnLabel[MT]], encCT: Writable[types.ColumnType[MT]]) = new Writable[LabelEntry[MT]] {
    def writeTo(buffer: WriteBuffer, ne: LabelEntry[MT]): Unit = {
      buffer.write(ne.label)
      buffer.write(ne.typ)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit decLabel: Readable[types.ColumnLabel[MT]], decCT: Readable[types.ColumnType[MT]]) = new Readable[LabelEntry[MT]] with LabelUniverse[MT] {
    def readFrom(buffer: ReadBuffer): LabelEntry[MT] = {
      LabelEntry(
        buffer.read[ColumnLabel](),
        buffer.read[CT]()
      )
    }
  }
}
case class NameEntry[+CT](name: ColumnName, @JsonKey("type") typ: CT)
object NameEntry {
  implicit def jEncode[CT: JsonEncode] = AutomaticJsonEncodeBuilder[NameEntry[CT]]
  implicit def jDecode[CT: JsonDecode] = AutomaticJsonDecodeBuilder[NameEntry[CT]]

  implicit def serialize[CT: Writable] = new Writable[NameEntry[CT]] {
    def writeTo(buffer: WriteBuffer, ne: NameEntry[CT]): Unit = {
      buffer.write(ne.name)
      buffer.write(ne.typ)
    }
  }

  implicit def deserialize[CT: Readable] = new Readable[NameEntry[CT]] {
    def readFrom(buffer: ReadBuffer): NameEntry[CT] = {
      NameEntry(
        buffer.read[ColumnName](),
        buffer.read[CT]()
      )
    }
  }
}

case class VirtualEntry[MT <: MetaTypes](name: ColumnName, label: AutoColumnLabel, typ: types.ColumnType[MT])
case class PhysicalEntry[MT <: MetaTypes](name: ColumnName, label: types.DatabaseColumnName[MT], typ: types.ColumnType[MT])

sealed trait ErasureWorkaround
object ErasureWorkaround {
  implicit val erasureWorkaround: ErasureWorkaround = null
}

sealed abstract class ScopeName
object ScopeName {
  case object Anonymous extends ScopeName
  case class Implicit(name: ResourceName) extends ScopeName
  case class Explicit(name: ResourceName) extends ScopeName
}

sealed trait Scope[MT <: MetaTypes] extends LabelUniverse[MT] {
  val label: AutoTableLabel
  // def types = schemaByName.iterator.map { case (_, e) => e.typ }.toSeq

}

object Scope {
  final class Virtual[MT <: MetaTypes] (
    val label: AutoTableLabel,
    schema: OrderedMap[AutoColumnLabel, NameEntry[types.ColumnType[MT]]]
  ) extends Scope[MT] {
    val schemaByName = OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (name, VirtualEntry[MT](name, label, typ)) }
    val schemaByLabel = OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (label, VirtualEntry[MT](name, label, typ)) }

    override def toString =
      new StringBuilder(label.toString).
        append(" -> ").
        append(schemaByLabel).
        toString
  }

  final class Physical[MT <: MetaTypes] (
    val tableName: types.DatabaseTableName[MT],
    val label: AutoTableLabel,
    schema: OrderedMap[types.DatabaseColumnName[MT], types.NameEntry[MT]]
  ) extends Scope[MT] {
    val schemaByName = OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (name, PhysicalEntry[MT](name, label, typ)) }
    val schemaByLabel = OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (label, PhysicalEntry[MT](name, label, typ)) }

    require(schemaByName.size == schemaByLabel.size, "Duplicate labels in schema")

    override def toString =
      new StringBuilder(label.toString).
        append(" -> ").
        append(schemaByLabel).
        toString
  }
}

// SQL naming rules:
//   You cannot have the same table alias more than once in a FROM clause
//   Unqualified names seek out the nearest definition
//     - all names in a FROM are equally near
//     - lateral subqueries introduce a step of distance

//
// SoQL naming rules:
//  You cannot have the same table alias more than once in a FROM clause
//     - unqualified-names are resolved relative to the first entry in the current query's FROM list, or from the inherited environment
//     - qualified names find the nearest qualifier

sealed abstract class Environment[MT <: MetaTypes](parent: Option[Environment[MT]]) {
  @tailrec
  final def lookup(name: ColumnName): Option[Environment.LookupResult[MT]] = {
    val candidate = lookupHere(name)
    if(candidate.isDefined) return candidate
    parent match {
      case Some(p) => p.lookup(name)
      case None => None
    }
  }

  @tailrec
  final def lookup(resource: ResourceName, name: ColumnName): Option[Environment.LookupResult[MT]] = {
    val candidate = lookupHere(resource, name)
    if(candidate.isDefined) return candidate
    parent match {
      case Some(p) => p.lookup(resource, name)
      case None => None
    }
  }

  protected def lookupHere(name: ColumnName): Option[Environment.LookupResult[MT]]
  protected def lookupHere(resource: ResourceName, name: ColumnName): Option[Environment.LookupResult[MT]]

  def extend: Environment[MT]

  def addScope(name: Option[ResourceName], scope: Scope[MT]): Either[AddScopeError, Environment[MT]]
}

object Environment {
  def empty[MT <: MetaTypes]: Environment[MT] = new EmptyEnvironment(None)

  sealed abstract class LookupResult[MT <: MetaTypes]
  object LookupResult {
    case class Virtual[MT <: MetaTypes](table: AutoTableLabel, column: AutoColumnLabel, typ: MT#ColumnType) extends LookupResult[MT]
    case class Physical[MT <: MetaTypes](tableName: types.DatabaseTableName[MT], table: AutoTableLabel, column: types.DatabaseColumnName[MT], typ: types.ColumnType[MT]) extends LookupResult[MT]
  }

  private class EmptyEnvironment[MT <: MetaTypes](parent: Option[Environment[MT]]) extends Environment(parent) with MetaTypeHelper[MT] {
    override def lookupHere(name: ColumnName) = None
    override def lookupHere(resource: ResourceName, name: ColumnName) = None

    override def extend: Environment[MT] = this

    override def addScope(name: Option[ResourceName], scope: Scope[MT]): Either[AddScopeError, Environment[MT]] =
      Right(new NonEmptyEnvironment(
        scope,
        name.fold(Map.empty[ResourceName, Scope[MT]]) { n => Map(n -> scope) },
        parent
      ))

    override def toString = "Environment()"
  }

  private class NonEmptyEnvironment[MT <: MetaTypes](
    implicitScope: Scope[MT],
    explicitScopes: Map[ResourceName, Scope[MT]],
    parent: Option[Environment[MT]]
  ) extends Environment(parent) {
    override def lookupHere(name: ColumnName) =
      implicitScope match {
        case virtual: Scope.Virtual[MT] =>
          virtual.schemaByName.get(name).map { entry =>
            LookupResult.Virtual[MT](virtual.label, entry.label, entry.typ)
          }
        case physical: Scope.Physical[MT] =>
          physical.schemaByName.get(name).map { entry =>
            LookupResult.Physical[MT](physical.tableName, physical.label, entry.label, entry.typ)
          }
      }

    override def toString = {
      new StringBuilder("Environment(\n  ").
        append(parent.toString).
        append(",\n  ").
        append(implicitScope).
        append(",\n  ").
        append(explicitScopes).
        append("\n)").
        toString
    }

    override def extend: Environment[MT] = new EmptyEnvironment(Some(this))

    override def lookupHere(resource: ResourceName, name: ColumnName) =
      explicitScopes.get(resource).flatMap { scope =>
        scope match {
          case virtual: Scope.Virtual[MT] =>
            virtual.schemaByName.get(name).map { entry =>
              LookupResult.Virtual[MT](virtual.label, entry.label, entry.typ)
            }
          case physical: Scope.Physical[MT] =>
            physical.schemaByName.get(name).map { entry =>
              LookupResult.Physical[MT](physical.tableName, physical.label, entry.label, entry.typ)
            }
        }
      }

    override def addScope(name: Option[ResourceName], scope: Scope[MT]): Either[AddScopeError, Environment[MT]] = {
      name match {
        case Some(rn) =>
          if(explicitScopes.contains(rn)) {
            return Left(AddScopeError.NameExists(rn))
          }
          Right(new NonEmptyEnvironment(
            implicitScope,
            explicitScopes + (rn -> scope),
            parent
          ))
        case None =>
          Left(AddScopeError.MultipleImplicit)
      }
    }
  }
}

sealed trait AddScopeError
object AddScopeError {
  case class NameExists(resourceName: ResourceName) extends AddScopeError
  case object MultipleImplicit extends AddScopeError
}
