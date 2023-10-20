package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.parsing.input.{Position, NoPosition}

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ResourceName, ScopedResourceName, ColumnName, HoleName, TableName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.{BinaryTree, Leaf, Compound}

trait TableFinder[MT <: MetaTypes] {
  // These are the things that need to be implemented by subclasses.
  // The "TableMap" representation will have to become more complex if
  // we support source-level CTEs, as a CTE will introduce the concept
  // of a _nested_ name scope.

  type ColumnType = MT#ColumnType
  type ResourceNameScope = MT#ResourceNameScope

  /** The way in which `parse` can fail.
    */
  type ParseError = ParserError[ResourceNameScope]

  /** Look up the given `name` in the given `scope` */
  protected def lookup(name: ScopedResourceName): Either[LookupError, FinderTableDescription]

  /** Parameters used to parse SoQL */
  protected val parserParameters: AbstractParser.Parameters = AbstractParser.defaultParameters

  /** The result of looking up a name, containing only the values
    * relevant to analysis.  Note this is very nearly the same as
    * UnparsedTableDescription but not _quite_ the same because of the
    * representation of Datasets' schemas. */
  sealed trait FinderTableDescription

  case class Ordering(column: types.DatabaseColumnName[MT], ascending: Boolean, nullLast: Boolean)

  case class DatasetColumnInfo(name: ColumnName, typ: ColumnType, hidden: Boolean)

  /** A base dataset, or a saved query which is being analyzed opaquely. */
  case class Dataset(
    databaseName: types.DatabaseTableName[MT],
    canonicalName: CanonicalName,
    schema: OrderedMap[types.DatabaseColumnName[MT], DatasetColumnInfo],
    ordering: Seq[Ordering],
    primaryKeys: Seq[Seq[types.DatabaseColumnName[MT]]]
  ) extends FinderTableDescription {
    require(schema.valuesIterator.map(_.name).toSet.size == schema.size)
    require(ordering.forall { ordering => schema.contains(ordering.column) })
    require(primaryKeys.forall { pk => pk.forall { pkCol => schema.contains(pkCol) } })

    private[analyzer2] def toParsed =
      TableDescription.Dataset[MT](
        databaseName,
        canonicalName,
        schema.map { case (cl, DatasetColumnInfo(cn, ct, hidden)) =>
          cl -> TableDescription.DatasetColumnInfo(cn, ct, hidden)
        },
        ordering.map { case Ordering(column, ascending, nullLast) =>
          TableDescription.Ordering(column, ascending, nullLast)
        },
        primaryKeys
      )
  }
  /** A saved query, with any parameters it (non-transitively!) defines. */
  case class Query(
    scope: ResourceNameScope,
    canonicalName: CanonicalName,
    basedOn: ResourceName,
    soql: String,
    parameters: Map[HoleName, ColumnType],
    hiddenColumns: Set[ColumnName]
  ) extends FinderTableDescription {
  }
  /** A saved table query ("UDF"), with any parameters it defines for itself. */
  case class TableFunction(
    scope: ResourceNameScope,
    canonicalName: CanonicalName,
    soql: String,
    parameters: OrderedMap[HoleName, ColumnType],
    hiddenColumns: Set[ColumnName]
  ) extends FinderTableDescription

  type ScopedResourceName = com.socrata.soql.environment.ScopedResourceName[ResourceNameScope]

  sealed trait LookupError
  object LookupError {
    case object NotFound extends LookupError
    case object PermissionDenied extends LookupError
  }

  type Result[T] = Either[TableFinderError[ResourceNameScope], T]

  type TableMap = com.socrata.soql.analyzer2.TableMap[MT]

  /** Find all tables referenced from the given SoQL.  No implicit context is assumed. */
  final def findTables(scope: ResourceNameScope, text: String, parameters: Map[HoleName, ColumnType]): Result[FoundTables[MT]] = {
    walkSoQL(scope, FoundTables.Standalone[MT](_, _, parameters), text, TableMap.empty, Nil)
  }

  /** Find all tables referenced from the given SoQL on name that provides an implicit context. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName, text: String, parameters: Map[HoleName, ColumnType]): Result[FoundTables[MT]] = {
    walkFromName(ScopedResourceName(scope, resourceName), None, NoPosition, TableMap.empty, Nil).flatMap { acc =>
      walkSoQL(scope, FoundTables.InContext[MT](resourceName, _, _, parameters), text, acc, Nil)
    }
  }

  /** Find all tables referenced from the given SoQL on name that
    * provides an implicit context, impersonating a saved query.
    * The intent here is that we're editing that saved query, so
    * we want qualified parameter references in the edited SoQL
    * to be found, and we want that parameter map to be editable.
    */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName, text: String, parameters: Map[HoleName, ColumnType], impersonating: CanonicalName): Result[FoundTables[MT]] = {
    walkFromName(ScopedResourceName(scope, resourceName), None, NoPosition, TableMap.empty, List(impersonating)).flatMap { acc =>
      walkSoQL(scope, FoundTables.InContextImpersonatingSaved[MT](resourceName, _, _, parameters, impersonating), text, acc, List(impersonating))
    }
  }

  /** Find all tables referenced from the given name. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName): Result[FoundTables[MT]] = {
    walkFromName(ScopedResourceName(scope, resourceName), None, NoPosition, TableMap.empty, Nil).map { acc =>
      FoundTables[MT](acc, scope, FoundTables.Saved(resourceName), parserParameters)
    }
  }

  // A helper that lifts the abstract function into the Result world
  private def doLookup(
    scopedName: ScopedResourceName, // the name of the thing we're looking up
    caller: Option[ScopedResourceName], // the name of the thing that is referencing `scopedName`
    pos: Position // the position of the reference within `caller`
  ): Result[TableDescription[MT]] = {
    lookup(scopedName) match {
      case Right(ds: Dataset) =>
        Right(ds.toParsed)
      case Right(Query(scope, canonicalName, basedOn, text, params, hiddenColumns)) =>
        parse(Some(scopedName), text, false).map(TableDescription.Query[MT](scope, canonicalName, basedOn, _, text, params, hiddenColumns))
      case Right(TableFunction(scope, canonicalName, text, params, hiddenColumns)) =>
        parse(Some(scopedName), text, true).map(TableDescription.TableFunction[MT](scope, canonicalName, _, text, params, hiddenColumns))
      case Left(LookupError.NotFound) =>
        Left(TableFinderError.NotFound(caller, pos, scopedName.name))
      case Left(LookupError.PermissionDenied) =>
        Left(TableFinderError.PermissionDenied(caller, pos, scopedName.name))
    }
  }

  private def parse(name: Option[ScopedResourceName], text: String, udfParamsAllowed: Boolean): Result[BinaryTree[ast.Select]] = {
    ParserUtil.parseInContext(name, text, parserParameters.copy(allowHoles = udfParamsAllowed))
  }

  private def walkFromName(scopedName: ScopedResourceName, caller: Option[ScopedResourceName], pos: Position, acc: TableMap, stack: List[CanonicalName]): Result[TableMap] = {
    acc.get(scopedName) match {
      case Some(desc) =>
        if(stack.contains(desc.canonicalName)) {
          Left(TableFinderError.RecursiveQuery(caller, pos, desc.canonicalName :: stack))
        } else {
          Right(acc)
        }
      case None =>
        if(Util.isSpecialTableName(scopedName)) {
          Right(acc)
        } else {
          for {
            desc <- doLookup(scopedName, caller, pos)
            acc <- walkDesc(scopedName, caller, pos, desc, acc + (scopedName -> desc), stack)
          } yield acc
        }
    }
  }

  def walkDesc(scopedName: ScopedResourceName, caller: Option[ScopedResourceName], pos: Position, desc: TableDescription[MT], acc: TableMap, stack: List[CanonicalName]): Result[TableMap] = {
    if(stack.contains(desc.canonicalName)) {
      return Left(TableFinderError.RecursiveQuery(caller, pos, desc.canonicalName :: stack))
    }
    desc match {
      case TableDescription.Dataset(_, _, _, _, _) => Right(acc)
      case TableDescription.Query(scope, canonicalName, basedOn, tree, _unparsed, _params, _hiddenColumns) =>
        for {
          acc <- walkFromName(ScopedResourceName(scope, basedOn), Some(scopedName), NoPosition, acc, canonicalName :: stack)
          acc <- walkTree(scope, Some(scopedName), tree, acc, canonicalName :: stack)
        } yield acc
      case TableDescription.TableFunction(scope, canonicalName, tree, _unparsed, _params, _hiddenColumns) =>
        walkTree(scope, Some(scopedName), tree, acc, canonicalName :: stack)
    }
  }

  // This walks anonymous in a context soql.  Named soql gets parsed in doLookup
  private def walkSoQL(scope: ResourceNameScope, context: (BinaryTree[ast.Select], String) => FoundTables.Query[MT], text: String, acc: TableMap, stack: List[CanonicalName]): Result[FoundTables[MT]] = {
    for {
      tree <- parse(None, text, udfParamsAllowed = false)
      acc <- walkTree(scope, None, tree, acc, stack)
    } yield {
      FoundTables[MT](acc, scope, context(tree, text), parserParameters)
    }
  }

  private def walkTree(scope: ResourceNameScope, treeName: Option[ScopedResourceName], bt: BinaryTree[ast.Select], acc: TableMap, stack: List[CanonicalName]): Result[TableMap] = {
    bt match {
      case Leaf(s) => walkSelect(scope, treeName, s, acc, stack)
      case c: Compound[ast.Select] =>
        for {
          acc <- walkTree(scope, treeName, c.left, acc, stack)
          acc <- walkTree(scope, treeName, c.right, acc, stack)
        } yield acc
    }
  }

  private def walkSelect(scope: ResourceNameScope, selectName: Option[ScopedResourceName], s: ast.Select, acc0: TableMap, stack: List[CanonicalName]): Result[TableMap] = {
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
      walkFromName(scopedName, selectName, NoPosition /* TODO: need position info from AST */, acc, stack) match {
        case Right(newAcc) =>
          acc = newAcc
        case Left(err) =>
          return Left(err)
      }
    }

    for(join <- joins) {
      val newAcc =
        join.from match {
          case ast.JoinTable(tn) =>
            walkFromName(ScopedResourceName(scope, ResourceName(tn.nameWithoutPrefix)), selectName, NoPosition /* TODO: need position info from AST */, acc, stack)
          case ast.JoinQuery(q, _) =>
            walkTree(scope, selectName, q, acc, stack)
          case ast.JoinFunc(f, _) =>
            walkFromName(ScopedResourceName(scope, ResourceName(f.nameWithoutPrefix)), selectName, NoPosition /* TODO: need position info from AST */, acc, stack)
        }

      newAcc match {
        case Right(newAcc) =>
          acc = newAcc
        case Left(e) =>
          return Left(e)
      }
    }

    Right(acc)
  }
}
