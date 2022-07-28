package com.socrata.soql.analyzer2

import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName}

case class LabelEntry[+CT](label: ColumnLabel, typ: CT)
case class NameEntry[+CT](name: ColumnName, typ: CT)

case class Entry[+CT](name: ColumnName, label: ColumnLabel, typ: CT)

sealed trait ErasureWorkaround
object ErasureWorkaround {
  implicit val erasureWorkaround: ErasureWorkaround = null
}

class Scope[+CT] private (
  val name: Option[ResourceName],
  val schemaByName: OrderedMap[ColumnName, Entry[CT]],
  val schemaByLabel: OrderedMap[ColumnLabel, Entry[CT]],
  val label: TableLabel
) {
  def types = schemaByName.iterator.map { case (_, e) => e.typ }.toSeq

  def this(name: Option[ResourceName], schema: OrderedMap[ColumnName, LabelEntry[CT]], label: TableLabel) = {
    this(
      name,
      OrderedMap() ++ schema.iterator.map { case (name, LabelEntry(label, typ)) => (name, Entry(name, label, typ)) },
      OrderedMap() ++ schema.iterator.map { case (name, LabelEntry(label, typ)) => (label, Entry(name, label, typ)) },
      label
    )
    require(schemaByName.size == schemaByLabel.size, "Duplicate labels in schema")
  }

  def this(name: Option[ResourceName], schema: Map[ColumnLabel, NameEntry[CT]], label: TableLabel)(implicit erasureWorkaround: ErasureWorkaround) = {
    this(
      name,
      OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (name, Entry(name, label, typ)) },
      OrderedMap() ++ schema.iterator.map { case (label, NameEntry(name, typ)) => (label, Entry(name, label, typ)) },
      label
    )
    require(schemaByName.size == schemaByLabel.size, "Duplicate name in schema")
  }

  def relabelled(newLabel: TableLabel) = new Scope(name, schemaByName, schemaByLabel, newLabel)
}

sealed abstract class LookupResult[+A];
object LookupResult {
  case object NotFound extends LookupResult[Nothing]
  case class Found[+A](a: A) extends LookupResult[A]
  case object Ambiguous extends LookupResult[Nothing]
}

class Environment[+CT] private (scopes: List[Scope[CT]]) {
  def this() = this(Nil)

  def extend[CT2 >: CT](scope: Scope[CT2]): Environment[CT2] = {
    new Environment(scope :: scopes)
  }

  def lookup(name: ColumnName): LookupResult[Column[CT]] = {
    lookupImpl(name, Function.const(true))
  }

  def lookup(resource: ResourceName, name: ColumnName): LookupResult[Column[CT]] = {
    lookupImpl(name, _.name == Some(resource))
  }

  def lookup(resource: Option[ResourceName], name: ColumnName): LookupResult[Column[CT]] = {
    resource match {
      case None => lookup(name)
      case Some(r) => lookup(r, name)
    }
  }

  private def lookupImpl(name: ColumnName, filter: Scope[CT] => Boolean): LookupResult[Column[CT]] = {
    val candidates = scopes.flatMap { scope =>
      if(filter(scope)) {
        scope.schemaByName.get(name).map { entry =>
          Column(scope.label, entry.label, entry.typ)
        }
      } else {
        None
      }
    }

    candidates match {
      case Seq() => LookupResult.NotFound
      case Seq(item) => LookupResult.Found(item)
      case _ => LookupResult.Ambiguous
    }
  }
}
