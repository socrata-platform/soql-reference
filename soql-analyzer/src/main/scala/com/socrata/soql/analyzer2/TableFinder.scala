package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ResourceName, ColumnName, HoleName}
import com.socrata.soql.{BinaryTree, Leaf, Compound}

trait TableFinder[CT] {
  // These are the things that need to be implemented by subclasses.
  // The "TableMap" representation will have to become more complex if
  // we support source-level CTEs, as a CTE will introduce the concept
  // of a _nested_ name scope.

  /** The way in which `parse` can fail.  This is probably a type from
    * soql.{standalone_exceptions,exceptions}.
    */
  type ParseError

  /** The way in which saved queries are scoped.  This is nearly opaque
    * as far as TableFinder is concerned, requiring only that a tuple
    * of it and ResourceName make a valid hash table key.  Looking up
    * a name will include the scope in which further transitively
    * referenced names can be looked up.
    *
    * It can just be "()" if we have a flat namespace, or for example a
    * domain + user for federation...
    */
  type ResourceNameScope

  /** Look up the given `name` in the given `scope` */
  protected def lookup(scope: ResourceNameScope, name: ResourceName): Either[LookupError, TableDescription]

  /** Parse the given SoQL */
  protected def parse(soql: String, udfParamsAllowed: Boolean): Either[ParseError, BinaryTree[ast.Select]]

  /** The result of looking up a name, containing only the values relevant to analysis. */
  sealed trait TableDescription
  /** A base dataset, or a saved query which is being analyzed opaquely. */
  case class Dataset(schema: Map[ColumnName, CT]) extends TableDescription with ParsedTableDescription
  /** A saved query, with any parameters it (non-transitively!) defines. */
  case class Query(scope: ResourceNameScope, soql: String, parameters: Map[HoleName, CT]) extends TableDescription
  /** A saved table query ("UDF"), with any parameters it defines for itself. */
  case class TableQuery(scope: ResourceNameScope, soql: String, parameters: OrderedMap[HoleName, CT]) extends TableDescription

  case class ScopedResourceName(scope: ResourceNameScope, name: ResourceName)

  sealed trait LookupError
  object LookupError {
    case object NotFound extends LookupError
    case object PermissionDenied extends LookupError
  }

  /** The result of a `findTables` call; either an error or a map from
    * ScopedResourceNames to ParsedTableDescriptions.
    */
  sealed trait Result[+T] {
    def map[U](f: T => U): Result[U]
    def flatMap[U](f: T => Result[U]): Result[U]
  }

  sealed trait Error extends Result[Nothing] {
    override final def map[U](f: Nothing => U): this.type = this
    override final def flatMap[U](f: Nothing => Result[U]): this.type = this
  }
  object Error {
    case class ParseError(name: Option[ScopedResourceName], error: TableFinder.this.ParseError) extends Error
    case class NotFound(name: ScopedResourceName) extends Error
    case class PermissionDenied(name: ScopedResourceName) extends Error
  }

  sealed trait ParsedTableDescription

  type TableMap = Map[ScopedResourceName, ParsedTableDescription]

  case class Success[T](value: T) extends Result[T] {
    override def map[U](f: T => U): Success[U] = Success(f(value))
    override def flatMap[U](f: T => Result[U]) = f(value)
  }

  case class ParsedQuery(scope: ResourceNameScope, parsed: BinaryTree[ast.Select], parameters: Map[HoleName, CT]) extends ParsedTableDescription
  case class ParsedTableQuery(scope: ResourceNameScope, parsed: BinaryTree[ast.Select], parameters: OrderedMap[HoleName, CT]) extends ParsedTableDescription

  /** Find all tables referenced from the given SoQL.  No implicit context is assumed. */
  final def findTables(scope: ResourceNameScope, text: String): Result[(BinaryTree[ast.Select], TableMap)] = {
    walkSoQL(scope, text, Map.empty)
  }

  /** Find all tables referenced from the given SoQL on name that provides an implicit context. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName, text: String): Result[(BinaryTree[ast.Select], TableMap)] = {
    findTables(scope, resourceName) match {
      case Success(acc) => walkSoQL(scope, text, acc)
      case err: Error => err
    }
  }

  /** Find all tables referenced from the given name. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName): Result[TableMap] = {
    walkFromName(ScopedResourceName(scope, resourceName), Map.empty)
  }

  // A pair of helpers that lift the abstract functions into the Result world
  private def doLookup(scopedName: ScopedResourceName): Result[ParsedTableDescription] = {
    lookup(scopedName.scope, scopedName.name) match {
      case Right(d: Dataset) => Success(d)
      case Right(Query(scope, text, params)) => doParse(Some(scopedName), text, false).map(ParsedQuery(scope, _, params))
      case Right(TableQuery(scope, text, params)) => doParse(Some(scopedName), text, true).map(ParsedTableQuery(scope, _, params))
      case Left(LookupError.NotFound) => Error.NotFound(scopedName)
      case Left(LookupError.PermissionDenied) => Error.PermissionDenied(scopedName)
    }
  }

  private def doParse(name: Option[ScopedResourceName], text: String, udfParamsAllowed: Boolean): Result[BinaryTree[ast.Select]] = {
    parse(text, udfParamsAllowed) match {
      case Right(tree) => Success(tree)
      case Left(err) => Error.ParseError(name, err)
    }
  }

  // Naming convention:
  //   Things which are called "walk" may end up making calls to "lookup" want will populate a TableMap
  //   Things which are called "convert" may not.

  private def walkFromName(scopedName: ScopedResourceName, acc: TableMap): Result[TableMap] = {
    if(acc.contains(scopedName)) {
      Success(acc)
    } else {
      for {
        desc <- doLookup(scopedName)
        acc <- walkDesc(desc, acc + (scopedName -> desc))
      } yield acc
    }
  }

  def walkDesc(desc: ParsedTableDescription, acc: TableMap): Result[TableMap] = {
    desc match {
      case _ : Dataset => Success(acc)
      case ParsedQuery(scope, tree, _params) => walkTree(scope, tree, acc)
      case ParsedTableQuery(scope, tree, _params) => walkTree(scope, tree, acc)
    }
  }

  // This walks anonymous soql.  Named soql gets parsed in doLookup
  private def walkSoQL(scope: ResourceNameScope, text: String, acc: TableMap): Result[(BinaryTree[ast.Select], TableMap)] = {
    for {
      tree <- doParse(None, text, udfParamsAllowed = false)
      acc <- walkTree(scope, tree, acc)
    } yield (tree, acc)
  }

  private def walkTree(scope: ResourceNameScope, bt: BinaryTree[ast.Select], acc: TableMap): Result[TableMap] = {
    bt match {
      case Leaf(s) => walkSelect(scope, s, acc)
      case c: Compound[ast.Select] =>
        for {
          acc <- walkTree(scope, c.left, acc)
          acc <- walkTree(scope, c.right, acc)
        } yield acc
    }
  }

  private def walkSelect(scope: ResourceNameScope, s: ast.Select, acc0: TableMap): Result[TableMap] = {
    val ast.Select(
      _distinct,
      _selection,
      from,
      joins,
      _where,
      _groupBys,
      _having,
      _orderBys,
      _limit,
      _offset,
      _search,
      _hints
    ) = s

    var acc = acc0

    for(tn <- from) {
      val scopedName = ScopedResourceName(scope, ResourceName(tn.nameWithoutPrefix))
      walkFromName(scopedName, acc) match {
        case Success(newAcc) =>
          acc = newAcc
        case e: Error =>
          return e
      }
    }

    for(join <- joins) {
      val newAcc =
        join.from match {
          case ast.JoinTable(tn) =>
            walkFromName(ScopedResourceName(scope, ResourceName(tn.nameWithoutPrefix)), acc)
          case ast.JoinQuery(q, _) =>
            walkTree(scope, q, acc)
          case ast.JoinFunc(f, _) =>
            walkFromName(ScopedResourceName(scope, ResourceName(f.nameWithoutPrefix)), acc)
        }

      newAcc match {
        case Success(newAcc) =>
          acc = newAcc
        case e: Error =>
          return e
      }
    }

    Success(acc)
  }
}
