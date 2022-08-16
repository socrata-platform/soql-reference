package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ResourceName, ColumnName, HoleName, TableName}
import com.socrata.soql.{BinaryTree, Leaf, Compound}

sealed trait ParsedTableDescription[+ResourceNameScope, +ColumnType] {
  private[analyzer2] def rewriteScopes[RNS >: ResourceNameScope, RNS2](scopeMap: Map[RNS, RNS2]): ParsedTableDescription[RNS2, ColumnType]
}
object ParsedTableDescription {
  case class Dataset[+ColumnType](
    name: DatabaseTableName,
    schema: OrderedMap[DatabaseColumnName, NameEntry[ColumnType]]
  ) extends ParsedTableDescription[Nothing, ColumnType] {
    private[analyzer2] def rewriteScopes[RNS, RNS2](scopeMap: Map[RNS, RNS2]) = this
  }
  case class Query[+ResourceNameScope, +ColumnType](
    scope: ResourceNameScope, // This scope is to resolve both basedOn and any tables referenced within the soql
    canonicalName: CanonicalName, // This is the canonical name of this query; it is assumed to be unique across scopes
    basedOn: ResourceName,
    parsed: BinaryTree[ast.Select],
    unparsed: String,
    parameters: Map[HoleName, ColumnType]
  ) extends ParsedTableDescription[ResourceNameScope, ColumnType] {
    private[analyzer2] def rewriteScopes[RNS >: ResourceNameScope, RNS2](scopeMap: Map[RNS, RNS2]) =
      copy(scope = scopeMap(scope))
  }
  case class TableFunction[+ResourceNameScope, +ColumnType](
    scope: ResourceNameScope, // This scope is to resolve any tables referenced within the soql
    canonicalName: CanonicalName, // This is the canonical name of this UDF; it is assumed to be unique across scopes
    parsed: BinaryTree[ast.Select],
    unparsed: String,
    parameters: OrderedMap[HoleName, ColumnType]
  ) extends ParsedTableDescription[ResourceNameScope, ColumnType] {
    private[analyzer2] def rewriteScopes[RNS >: ResourceNameScope, RNS2](scopeMap: Map[RNS, RNS2]) =
      copy(scope = scopeMap(scope))
  }
}

class TableMap[ResourceNameScope, +ColumnType](private val underlying: Map[(ResourceNameScope, ResourceName), ParsedTableDescription[ResourceNameScope, ColumnType]]) extends AnyVal {
  type ScopedResourceName = (ResourceNameScope, ResourceName)

  def contains(name: ScopedResourceName) = underlying.contains(name)
  def get(name: ScopedResourceName) = underlying.get(name)
  def getOrElse[CT2 >: ColumnType](name: ScopedResourceName)(orElse: => ParsedTableDescription[ResourceNameScope, CT2]) = underlying.getOrElse(name, orElse)

  def +[CT2 >: ColumnType](kv: (ScopedResourceName, ParsedTableDescription[ResourceNameScope, CT2])): TableMap[ResourceNameScope, CT2] =
    new TableMap(underlying + kv)

  def find(scope: ResourceNameScope, name: ResourceName): ParsedTableDescription[ResourceNameScope, ColumnType] = {
    getOrElse((scope, name)) {
      throw new NoSuchElementException(s"TableMap: No such key: $scope:$name")
    }
  }

  def descriptions = underlying.valuesIterator

  private[analyzer2] def rewriteScopes(topLevel: ResourceNameScope): (TableMap[Int, ColumnType], Map[Int, ResourceNameScope], Map[ResourceNameScope, Int]) = {
    val oldToNew = (underlying.keysIterator.map(_._1) ++ Iterator.single(topLevel)).zipWithIndex.toMap
    val newToOld = oldToNew.iterator.map(_.swap).toMap
    val newMap =
      underlying.iterator.map { case ((rns, rn), desc) =>
        (oldToNew(rns), rn) -> desc.rewriteScopes(oldToNew)
      }.toMap

    (new TableMap(newMap), newToOld, oldToNew)
  }

  override def toString = "TableMap(" + underlying + ")"
}
object TableMap {
  def empty[ResourceNameScope] = new TableMap[ResourceNameScope, Nothing](Map.empty)
}

case class FoundTables[ResourceNameScope, +ColumnType](
  tableMap: TableMap[ResourceNameScope, ColumnType],
  initialScope: ResourceNameScope,
  initialQuery: FoundTables.Query
) {
  val knownUserParameters: Map[CanonicalName, Map[HoleName, ColumnType]] =
    tableMap.descriptions.foldLeft(Map.empty[CanonicalName, Map[HoleName, ColumnType]]) { (acc, desc) =>
      desc match {
        case _ : ParsedTableDescription.Dataset[_] | _ : ParsedTableDescription.TableFunction[_, _] => acc
        case q: ParsedTableDescription.Query[_, ColumnType] => acc + (q.canonicalName -> q.parameters)
      }
    }

  // This lets you convert resource scope names to a simplified form
  // if your resource scope names in one location have semantic
  // meaning that you don't care to serialize.  You also get a map
  // from the meaningless name to the meaningful one so if you want to
  // (for example) translate an error from the analyzer back into the
  // meaningful form, you can do that.
  lazy val (withSimplifiedScopes, simplifiedScopeMap) = locally {
    val (newMap, newToOld, oldToNew) = tableMap.rewriteScopes(initialScope)

    val newFT = FoundTables(
      newMap,
      oldToNew(initialScope),
      initialQuery
    )

    (newMap, newToOld)
  }
}

object FoundTables {
  sealed abstract class Query
  case class Saved(name: ResourceName) extends Query
  case class InContext(parent: ResourceName, soql: BinaryTree[ast.Select]) extends Query
  case class InContextImpersonatingSaved(parent: ResourceName, soql: BinaryTree[ast.Select], fake: CanonicalName) extends Query
  case class Standalone(soql: BinaryTree[ast.Select]) extends Query
}

trait TableFinder {
  // These are the things that need to be implemented by subclasses.
  // The "TableMap" representation will have to become more complex if
  // we support source-level CTEs, as a CTE will introduce the concept
  // of a _nested_ name scope.

  type ColumnType

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
  case class Dataset(
    databaseName: DatabaseTableName,
    schema: OrderedMap[ColumnName, ColumnType]
  ) extends TableDescription {
    private[analyzer2] def toParsed =
      ParsedTableDescription.Dataset(
        databaseName,
        schema.map { case (cn, ct) =>
          // This is a little icky...?  But at this point we're in
          // the user-provided names world, so this is at least a
          // _predictable_ key to use as a "database column name"
          // before we get to the point of moving over to the
          // actual-database-names world.
          DatabaseColumnName(cn.caseFolded) -> NameEntry(cn, ct)
        }
      )
  }
  /** A saved query, with any parameters it (non-transitively!) defines. */
  case class Query(
    scope: ResourceNameScope,
    canonicalName: CanonicalName,
    basedOn: ResourceName,
    soql: String,
    parameters: Map[HoleName, ColumnType]
  ) extends TableDescription {
  }
  /** A saved table query ("UDF"), with any parameters it defines for itself. */
  case class TableFunction(
    scope: ResourceNameScope,
    canonicalName: CanonicalName,
    soql: String,
    parameters: OrderedMap[HoleName, ColumnType]
  ) extends TableDescription

  type ScopedResourceName = (ResourceNameScope, ResourceName)

  sealed trait LookupError
  object LookupError {
    case object NotFound extends LookupError
    case object PermissionDenied extends LookupError
  }

  /** The result of a `findTables` call.
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

  type TableMap = com.socrata.soql.analyzer2.TableMap[ResourceNameScope, ColumnType]

  case class Success[T](value: T) extends Result[T] {
    override def map[U](f: T => U): Success[U] = Success(f(value))
    override def flatMap[U](f: T => Result[U]) = f(value)
  }

  /** Find all tables referenced from the given SoQL.  No implicit context is assumed. */
  final def findTables(scope: ResourceNameScope, text: String): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    walkSoQL(scope, FoundTables.Standalone, text, TableMap.empty)
  }

  /** Find all tables referenced from the given SoQL on name that provides an implicit context. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName, text: String): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    walkFromName((scope, resourceName), TableMap.empty) match {
      case Success(acc) => walkSoQL(scope, FoundTables.InContext(resourceName, _), text, acc)
      case err: Error => err
    }
  }

  /** Find all tables referenced from the given SoQL on name that
    * provides an implicit context, impersonating a saved query. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName, text: String, impersonating: CanonicalName): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    walkFromName((scope, resourceName), TableMap.empty) match {
      case Success(acc) => walkSoQL(scope, FoundTables.InContextImpersonatingSaved(resourceName, _, impersonating), text, acc)
      case err: Error => err
    }
  }

  /** Find all tables referenced from the given name. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    walkFromName((scope, resourceName), TableMap.empty).map { acc =>
      FoundTables(acc, scope, FoundTables.Saved(resourceName))
    }
  }

  // A pair of helpers that lift the abstract functions into the Result world
  private def doLookup(scopedName: ScopedResourceName): Result[ParsedTableDescription[ResourceNameScope, ColumnType]] = {
    lookup(scopedName._1, scopedName._2) match {
      case Right(ds: Dataset) =>
        Success(ds.toParsed)
      case Right(Query(scope, canonicalName, basedOn, text, params)) =>
        doParse(Some(scopedName), text, false).map(ParsedTableDescription.Query(scope, canonicalName, basedOn, _, text, params))
      case Right(TableFunction(scope, canonicalName, text, params)) => doParse(Some(scopedName), text, true).map(ParsedTableDescription.TableFunction(scope, canonicalName, _, text, params))
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

  private def walkFromName(scopedName: ScopedResourceName, acc: TableMap): Result[TableMap] = {
    if(acc.contains(scopedName) || isSpecialTableName(scopedName)) {
      Success(acc)
    } else {
      for {
        desc <- doLookup(scopedName)
        acc <- walkDesc(desc, acc + (scopedName -> desc))
      } yield acc
    }
  }

  private def isSpecialTableName(scopedName: ScopedResourceName): Boolean = {
    val (_, name) = scopedName
    val prefixedName = TableName.SodaFountainPrefix + name.caseFolded
    TableName.reservedNames.contains(prefixedName)
  }

  def walkDesc(desc: ParsedTableDescription[ResourceNameScope, ColumnType], acc: TableMap): Result[TableMap] = {
    desc match {
      case ParsedTableDescription.Dataset(_, _) => Success(acc)
      case ParsedTableDescription.Query(scope, _canonicalName, basedOn, tree, _unparsed, _params) =>
        for {
          acc <- walkFromName((scope, basedOn), acc)
          acc <- walkTree(scope, tree, acc)
        } yield acc
      case ParsedTableDescription.TableFunction(scope, _canonicalName, tree, _unparsed, _params) => walkTree(scope, tree, acc)
    }
  }

  // This walks anonymous soql.  Named soql gets parsed in doLookup
  private def walkSoQL(scope: ResourceNameScope, context: BinaryTree[ast.Select] => FoundTables.Query, text: String, acc: TableMap): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    for {
      tree <- doParse(None, text, udfParamsAllowed = false)
      acc <- walkTree(scope, tree, acc)
    } yield {
      FoundTables(acc, scope, context(tree))
    }
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
      val scopedName = (scope, ResourceName(tn.nameWithoutPrefix))
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
            walkFromName((scope, ResourceName(tn.nameWithoutPrefix)), acc)
          case ast.JoinQuery(q, _) =>
            walkTree(scope, q, acc)
          case ast.JoinFunc(f, _) =>
            walkFromName((scope, ResourceName(f.nameWithoutPrefix)), acc)
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
