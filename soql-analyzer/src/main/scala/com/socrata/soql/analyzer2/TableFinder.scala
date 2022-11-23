package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ResourceName, ColumnName, HoleName, TableName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.{BinaryTree, Leaf, Compound}

trait TableFinder {
  // These are the things that need to be implemented by subclasses.
  // The "TableMap" representation will have to become more complex if
  // we support source-level CTEs, as a CTE will introduce the concept
  // of a _nested_ name scope.

  type ColumnType

  /** The way in which `parse` can fail.  This is probably a type from
    * soql.{standalone_exceptions,exceptions}.
    */
  type ParseError = LexerParserException

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
  protected def lookup(name: QualifiedResourceName): Either[LookupError, FinderTableDescription]

  /** Parameters used to parse SoQL */
  protected val parserParameters: AbstractParser.Parameters = AbstractParser.defaultParameters

  /** The result of looking up a name, containing only the values
    * relevant to analysis.  Note this is very nearly the same as
    * UnparsedTableDescription but not _quite_ the same because of the
    * representation of Datasets' schemas. */
  sealed trait FinderTableDescription

  case class Ordering(column: ColumnName, ascending: Boolean)

  /** A base dataset, or a saved query which is being analyzed opaquely. */
  case class Dataset(
    databaseName: DatabaseTableName,
    canonicalName: CanonicalName,
    schema: OrderedMap[ColumnName, ColumnType],
    ordering: Seq[Ordering]
  ) extends FinderTableDescription {
    require(ordering.forall { ordering => schema.contains(ordering.column) })

    private[analyzer2] def toParsed =
      TableDescription.Dataset(
        databaseName,
        canonicalName,
        schema.map { case (cn, ct) =>
          // This is a little icky...?  But at this point we're in
          // the user-provided names world, so this is at least a
          // _predictable_ key to use as a "database column name"
          // before we get to the point of moving over to the
          // actual-database-names world.
          DatabaseColumnName(cn.caseFolded) -> NameEntry(cn, ct)
        },
        ordering.map { case Ordering(column, ascending) =>
          TableDescription.Ordering(DatabaseColumnName(column.caseFolded), ascending)
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
  ) extends FinderTableDescription {
  }
  /** A saved table query ("UDF"), with any parameters it defines for itself. */
  case class TableFunction(
    scope: ResourceNameScope,
    canonicalName: CanonicalName,
    soql: String,
    parameters: OrderedMap[HoleName, ColumnType]
  ) extends FinderTableDescription

  type QualifiedResourceName = com.socrata.soql.analyzer2.QualifiedResourceName[ResourceNameScope]

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
    case class ParseError(name: Option[QualifiedResourceName], error: TableFinder.this.ParseError) extends Error
    case class NotFound(name: QualifiedResourceName) extends Error
    case class PermissionDenied(name: QualifiedResourceName) extends Error
    case class RecursiveQuery(canonicalName: Seq[CanonicalName]) extends Error
  }

  type TableMap = com.socrata.soql.analyzer2.TableMap[ResourceNameScope, ColumnType]

  case class Success[T](value: T) extends Result[T] {
    override def map[U](f: T => U): Success[U] = Success(f(value))
    override def flatMap[U](f: T => Result[U]) = f(value)
  }

  /** Find all tables referenced from the given SoQL.  No implicit context is assumed. */
  final def findTables(scope: ResourceNameScope, text: String, parameters: Map[HoleName, ColumnType]): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    walkSoQL(scope, FoundTables.Standalone(_, _, parameters), text, TableMap.empty, Nil)
  }

  /** Find all tables referenced from the given SoQL on name that provides an implicit context. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName, text: String, parameters: Map[HoleName, ColumnType]): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    walkFromName(QualifiedResourceName(scope, resourceName), TableMap.empty, Nil) match {
      case Success(acc) => walkSoQL(scope, FoundTables.InContext(resourceName, _, _, parameters), text, acc, Nil)
      case err: Error => err
    }
  }

  /** Find all tables referenced from the given SoQL on name that
    * provides an implicit context, impersonating a saved query.
    * The intent here is that we're editing that saved query, so
    * we want qualified parameter references in the edited SoQL
    * to be found, and we want that parameter map to be editable.
    */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName, text: String, parameters: Map[HoleName, ColumnType], impersonating: CanonicalName): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    walkFromName(QualifiedResourceName(scope, resourceName), TableMap.empty, List(impersonating)) match {
      case Success(acc) => walkSoQL(scope, FoundTables.InContextImpersonatingSaved(resourceName, _, _, parameters, impersonating), text, acc, List(impersonating))
      case err: Error => err
    }
  }

  /** Find all tables referenced from the given name. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    walkFromName(QualifiedResourceName(scope, resourceName), TableMap.empty, Nil).map { acc =>
      FoundTables(acc, scope, FoundTables.Saved(resourceName), parserParameters)
    }
  }

  // A pair of helpers that lift the abstract functions into the Result world
  private def doLookup(scopedName: QualifiedResourceName): Result[TableDescription[ResourceNameScope, ColumnType]] = {
    lookup(scopedName) match {
      case Right(ds: Dataset) =>
        Success(ds.toParsed)
      case Right(Query(scope, canonicalName, basedOn, text, params)) =>
        parse(Some(scopedName), text, false).map(TableDescription.Query(scope, canonicalName, basedOn, _, text, params))
      case Right(TableFunction(scope, canonicalName, text, params)) => parse(Some(scopedName), text, true).map(TableDescription.TableFunction(scope, canonicalName, _, text, params))
      case Left(LookupError.NotFound) => Error.NotFound(scopedName)
      case Left(LookupError.PermissionDenied) => Error.PermissionDenied(scopedName)
    }
  }

  private def parse(name: Option[QualifiedResourceName], text: String, udfParamsAllowed: Boolean): Result[BinaryTree[ast.Select]] = {
    ParserUtil(text, parserParameters.copy(allowHoles = udfParamsAllowed)) match {
      case Right(tree) => Success(tree)
      case Left(err) => Error.ParseError(name, err)
    }
  }

  private def walkFromName(scopedName: QualifiedResourceName, acc: TableMap, stack: List[CanonicalName]): Result[TableMap] = {
    acc.get(scopedName) match {
      case Some(desc) =>
        if(stack.contains(desc.canonicalName)) {
          Error.RecursiveQuery(desc.canonicalName :: stack)
        } else {
          Success(acc)
        }
      case None =>
        if(isSpecialTableName(scopedName)) {
          Success(acc)
        } else {
          for {
            desc <- doLookup(scopedName)
            acc <- walkDesc(desc, acc + (scopedName -> desc), stack)
          } yield acc
        }
    }
  }

  private def isSpecialTableName(scopedName: QualifiedResourceName): Boolean = {
    val QualifiedResourceName(_, name) = scopedName
    val prefixedName = TableName.SodaFountainPrefix + name.caseFolded
    TableName.reservedNames.contains(prefixedName)
  }

  def walkDesc(desc: TableDescription[ResourceNameScope, ColumnType], acc: TableMap, stack: List[CanonicalName]): Result[TableMap] = {
    if(stack.contains(desc.canonicalName)) {
      return Error.RecursiveQuery(desc.canonicalName :: stack)
    }
    desc match {
      case TableDescription.Dataset(_, _, _, _) => Success(acc)
      case TableDescription.Query(scope, canonicalName, basedOn, tree, _unparsed, _params) =>
        for {
          acc <- walkFromName(QualifiedResourceName(scope, basedOn), acc, canonicalName :: stack)
          acc <- walkTree(scope, tree, acc, canonicalName :: stack)
        } yield acc
      case TableDescription.TableFunction(scope, canonicalName, tree, _unparsed, _params) =>
        walkTree(scope, tree, acc, canonicalName :: stack)
    }
  }

  // This walks anonymous soql.  Named soql gets parsed in doLookup
  private def walkSoQL(scope: ResourceNameScope, context: (BinaryTree[ast.Select], String) => FoundTables.Query[ColumnType], text: String, acc: TableMap, stack: List[CanonicalName]): Result[FoundTables[ResourceNameScope, ColumnType]] = {
    for {
      tree <- parse(None, text, udfParamsAllowed = false)
      acc <- walkTree(scope, tree, acc, stack)
    } yield {
      FoundTables(acc, scope, context(tree, text), parserParameters)
    }
  }

  private def walkTree(scope: ResourceNameScope, bt: BinaryTree[ast.Select], acc: TableMap, stack: List[CanonicalName]): Result[TableMap] = {
    bt match {
      case Leaf(s) => walkSelect(scope, s, acc, stack)
      case c: Compound[ast.Select] =>
        for {
          acc <- walkTree(scope, c.left, acc, stack)
          acc <- walkTree(scope, c.right, acc, stack)
        } yield acc
    }
  }

  private def walkSelect(scope: ResourceNameScope, s: ast.Select, acc0: TableMap, stack: List[CanonicalName]): Result[TableMap] = {
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
      val scopedName = QualifiedResourceName(scope, ResourceName(tn.nameWithoutPrefix))
      walkFromName(scopedName, acc, stack) match {
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
            walkFromName(QualifiedResourceName(scope, ResourceName(tn.nameWithoutPrefix)), acc, stack)
          case ast.JoinQuery(q, _) =>
            walkTree(scope, q, acc, stack)
          case ast.JoinFunc(f, _) =>
            walkFromName(QualifiedResourceName(scope, ResourceName(f.nameWithoutPrefix)), acc, stack)
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
