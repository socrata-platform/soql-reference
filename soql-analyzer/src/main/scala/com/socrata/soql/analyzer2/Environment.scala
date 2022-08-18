package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, JsonKey}

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName}

case class LabelEntry[+CT](label: ColumnLabel, @JsonKey("type") typ: CT)
object LabelEntry {
  implicit def jEncode[CT: JsonEncode] = AutomaticJsonEncodeBuilder[LabelEntry[CT]]
  implicit def jDecode[CT: JsonDecode] = AutomaticJsonDecodeBuilder[LabelEntry[CT]]
}
case class NameEntry[+CT](name: ColumnName, @JsonKey("type") typ: CT)
object NameEntry {
  implicit def jEncode[CT: JsonEncode] = AutomaticJsonEncodeBuilder[NameEntry[CT]]
  implicit def jDecode[CT: JsonDecode] = AutomaticJsonDecodeBuilder[NameEntry[CT]]
}

case class Entry[+CT](name: ColumnName, label: ColumnLabel, typ: CT)

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

class Scope[+CT] private (
  val schemaByName: OrderedMap[ColumnName, Entry[CT]],
  val schemaByLabel: OrderedMap[ColumnLabel, Entry[CT]],
  val label: TableLabel
) {
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
  def apply[CT](schema: OrderedMap[ColumnName, LabelEntry[CT]], label: TableLabel) = {
    new Scope(
      OrderedMap() ++ schema.iterator.map { case (name, LabelEntry(label, typ)) => (name, Entry(name, label, typ)) },
      OrderedMap() ++ schema.iterator.map { case (name, LabelEntry(label, typ)) => (label, Entry(name, label, typ)) },
      label
    )
  }

  def apply[L <: ColumnLabel, CT](schema: OrderedMap[L, NameEntry[CT]], label: TableLabel)(implicit erasureWorkaround: ErasureWorkaround) = {
    new Scope(
      OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (name, Entry(name, label, typ)) },
      OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (label, Entry(name, label, typ)) },
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

sealed abstract class Environment[+CT](parent: Option[Environment[CT]]) {
  @tailrec
  final def lookup(name: ColumnName): Option[Environment.LookupResult[CT]] = {
    val candidate = lookupHere(name)
    if(candidate.isDefined) return candidate
    parent match {
      case Some(p) => p.lookup(name)
      case None => None
    }
  }

  @tailrec
  final def lookup(resource: ResourceName, name: ColumnName): Option[Environment.LookupResult[CT]] = {
    val candidate = lookupHere(resource, name)
    if(candidate.isDefined) return candidate
    parent match {
      case Some(p) => p.lookup(resource, name)
      case None => None
    }
  }

  protected def lookupHere(name: ColumnName): Option[Environment.LookupResult[CT]]
  protected def lookupHere(resource: ResourceName, name: ColumnName): Option[Environment.LookupResult[CT]]

  def extend: Environment[CT]

  def addScope[CT2 >: CT](name: Option[ResourceName], scope: Scope[CT2]): Either[AddScopeError, Environment[CT2]]
}

object Environment {
  val empty: Environment[Nothing] = new EmptyEnvironment(None)

  case class LookupResult[+CT](table: TableLabel, column: ColumnLabel, typ: CT)

  private class EmptyEnvironment[CT](parent: Option[Environment[CT]]) extends Environment(parent) {
    override def lookupHere(name: ColumnName) = None
    override def lookupHere(resource: ResourceName, name: ColumnName) = None

    override def extend: Environment[CT] = this

    override def addScope[CT2 >: CT](name: Option[ResourceName], scope: Scope[CT2]): Either[AddScopeError, Environment[CT2]] =
      Right(new NonEmptyEnvironment(
        scope,
        name.fold(Map.empty[ResourceName, Scope[CT2]]) { n => Map(n -> scope) },
        parent
      ))

    override def toString = "Environment()"
  }

  private class NonEmptyEnvironment[+CT](
    implicitScope: Scope[CT],
    explicitScopes: Map[ResourceName, Scope[CT]],
    parent: Option[Environment[CT]]
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

    override def extend: Environment[CT] = new EmptyEnvironment(Some(this))

    override def lookupHere(resource: ResourceName, name: ColumnName) =
      for {
        scope <- explicitScopes.get(resource)
        entry <- scope.schemaByName.get(name)
      } yield {
        LookupResult(scope.label, entry.label, entry.typ)
      }

    override def addScope[CT2 >: CT](name: Option[ResourceName], scope: Scope[CT2]): Either[AddScopeError, Environment[CT2]] = {
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
