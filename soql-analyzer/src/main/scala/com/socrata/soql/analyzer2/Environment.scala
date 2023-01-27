package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, JsonKey}

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName}
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}

case class LabelEntry[MT <: MetaTypes](label: ColumnLabel[MT#DatabaseColumnNameImpl], @JsonKey("type") typ: MT#CT)
object LabelEntry {
  implicit def jEncode[MT <: MetaTypes](implicit encLabel: JsonEncode[MT#DatabaseColumnNameImpl], encCT: JsonEncode[MT#CT]) = AutomaticJsonEncodeBuilder[LabelEntry[MT]]

  implicit def jDecode[MT <: MetaTypes](implicit decLabel: JsonDecode[MT#DatabaseColumnNameImpl], decCT: JsonDecode[MT#CT]) = AutomaticJsonDecodeBuilder[LabelEntry[MT]]

  implicit def serialize[MT <: MetaTypes](implicit encLabel: Writable[MT#DatabaseColumnNameImpl], encCT: Writable[MT#CT]) = new Writable[LabelEntry[MT]] {
    def writeTo(buffer: WriteBuffer, ne: LabelEntry[MT]): Unit = {
      buffer.write(ne.label)
      buffer.write(ne.typ)
    }
  }

  implicit def deserialize[MT <: MetaTypes](implicit decLabel: Readable[MT#DatabaseColumnNameImpl], decCT: Readable[MT#CT]) = new Readable[LabelEntry[MT]] {
    def readFrom(buffer: ReadBuffer): LabelEntry[MT] = {
      LabelEntry(
        buffer.read[ColumnLabel[MT#DatabaseColumnNameImpl]](),
        buffer.read[MT#CT]()
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

case class Entry[MT <: MetaTypes](name: ColumnName, label: ColumnLabel[MT#DatabaseColumnNameImpl], typ: MT#CT)

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

class Scope[MT <: MetaTypes] private (
  val schemaByName: OrderedMap[ColumnName, Entry[MT]],
  val schemaByLabel: OrderedMap[ColumnLabel[MT#DatabaseColumnNameImpl], Entry[MT]],
  val label: TableLabel[MT#DatabaseTableNameImpl]
) extends LabelHelper[MT] {
  require(schemaByName.size == schemaByLabel.size, "Duplicate labels in schema")

  def types = schemaByName.iterator.map { case (_, e) => e.typ }.toSeq
  def relabelled(newLabel: TableLabel) = new Scope(schemaByName, schemaByLabel, newLabel)

  override def toString =
    new StringBuilder(label.toString).
      append(" -> ").
      append(schemaByLabel).
      toString
}

object Scope {
  def fromNames[MT <: MetaTypes](schema: OrderedMap[ColumnName, LabelEntry[MT]], label: TableLabel[MT#DatabaseTableNameImpl]) = {
    new Scope[MT](
      OrderedMap() ++ schema.iterator.map { case (name, LabelEntry(label, typ)) => (name, Entry[MT](name, label, typ)) },
      OrderedMap() ++ schema.iterator.map { case (name, LabelEntry(label, typ)) => (label, Entry[MT](name, label, typ)) },
      label
    )
  }

  def fromLabels[MT <: MetaTypes, L[CNI] <: ColumnLabel[CNI]](schema: OrderedMap[L[MT#DatabaseColumnNameImpl], NameEntry[MT#CT]], label: TableLabel[MT#DatabaseTableNameImpl])(implicit erasureWorkaround: ErasureWorkaround) = {
    new Scope[MT](
      OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (name, Entry[MT](name, label, typ)) },
      OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (label, Entry[MT](name, label, typ)) },
      label
    )
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

  case class LookupResult[MT <: MetaTypes](table: TableLabel[MT#DatabaseTableNameImpl], column: ColumnLabel[MT#DatabaseColumnNameImpl], typ: MT#CT)

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
      implicitScope.schemaByName.get(name).map { entry =>
        LookupResult(implicitScope.label, entry.label, entry.typ)
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
      for {
        scope <- explicitScopes.get(resource)
        entry <- scope.schemaByName.get(name)
      } yield {
        LookupResult(scope.label, entry.label, entry.typ)
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
