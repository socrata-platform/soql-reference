package com.socrata.soql.analyzer2

import scala.annotation.tailrec
import scala.util.parsing.input.{Position, NoPosition}

import com.rojoma.json.v3.ast.JValue

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ResourceName, ScopedResourceName, Source, ColumnName, HoleName, TableName}
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

  case class DatasetColumnInfo(name: ColumnName, typ: ColumnType, hidden: Boolean, hint: Option[JValue])

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
        schema.map { case (cl, DatasetColumnInfo(cn, ct, hidden, hint)) =>
          cl -> TableDescription.DatasetColumnInfo(cn, ct, hidden, hint)
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
    hiddenColumns: Set[ColumnName],
    outputColumnHints: Map[ColumnName, JValue]
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

  //  This is a callstack _excluding_ the current query, and as a
  //  result it may be empty.
  private sealed trait CallerStack {
    def callstack: Seq[CanonicalName]
    // This is the point in the Caller's source that made the call to
    // the current query.
    def callerSource: Option[Source[ResourceNameScope]]

    // This is the topmost point in the callstack where the loop started
    def loopSource(canonicalName: CanonicalName): Option[Source[ResourceNameScope]]
  }
  private object CallerStack {
    case object Empty extends CallerStack {
      def callstack: Seq[CanonicalName] = Nil
      def callerSource = None
      def loopSource(canonicalName: CanonicalName): Option[Source[ResourceNameScope]] = None
    }
    // This caller explicity named its callee
    case class Explicit(stack: CallStack, reference: Position) extends CallerStack {
      def callstack: Seq[CanonicalName] = stack.callstack
      def callerSource = Some(stack.source(reference))
      def loopSource(canonicalName: CanonicalName) = stack.loopSource(canonicalName, reference)
    }
    // This caller implicitly named its callee (i.e., it was a query
    // on a saved query or dataset)
    case class Implicit(stack: CallStack) extends CallerStack {
      def callstack: Seq[CanonicalName] = stack.callstack
      // This is a little weird, and I'm not 100% sure I like it?  It
      // defines the source of an implicit reference to a parent to be
      // "the child, but no position", which is reasonable but doesn't
      // sit well?  The obvious alternative ("no source") also doesn't
      // sit very well
      def callerSource = Some(stack.source(NoPosition))
      def loopSource(canonicalName: CanonicalName) = stack.loopSource(canonicalName, NoPosition)
    }
  }
  // This is a callstack _including_ the current query (and as a
  // result it is never "empty")
  private sealed trait CallStack {
    def caller: CallerStack
    def callstack: Seq[CanonicalName]
    def source(pos: Position): Source[ResourceNameScope]
    def loopSource(canonicalName: CanonicalName, pos: Position): Option[Source[ResourceNameScope]]
  }
  private object CallStack {
    case class Anonymous(impersonating: Option[CanonicalName]) extends CallStack {
      override def caller = CallerStack.Empty
      override def callstack = impersonating.toSeq
      override def source(pos: Position) = Source.Anonymous(pos)
      override def loopSource(canonicalName: CanonicalName, pos: Position): Option[Source[ResourceNameScope]] =
        impersonating match {
          case Some(i) if i == canonicalName => Some(source(pos))
          case _ => None
        }
    }
    case class Saved(srn: ScopedResourceName, canonicalName: CanonicalName, caller: CallerStack) extends CallStack {
      override def callstack = {
        val result = Vector.newBuilder[CanonicalName]
        @tailrec
        def loop(ptr: CallStack): Unit =
          ptr match {
            case Saved(_, cn, tail) =>
              result += cn
              tail match {
                case CallerStack.Implicit(stack) =>
                  loop(stack)
                case CallerStack.Explicit(stack, _) =>
                  loop(stack)
                case CallerStack.Empty =>
                  // done
              }
            case other =>
              result ++= other.callstack
          }
        loop(this)
        result.result()
      }
      override def source(pos: Position) = Source.Saved(srn, pos)
      override def loopSource(canonicalName: CanonicalName, pos: Position) = {
        if(this.canonicalName == canonicalName) {
          Some(source(pos))
        } else {
          caller.loopSource(canonicalName)
        }
      }
    }
  }

  /** Find all tables referenced from the given SoQL.  No implicit context is assumed. */
  final def findTables(scope: ResourceNameScope, text: String, parameters: Map[HoleName, ColumnType]): Result[FoundTables[MT]] = {
    walkSoQL(scope, FoundTables.Standalone[MT](_, _, parameters), text, TableMap.empty, CallStack.Anonymous(None))
  }

  /** Find all tables referenced from the given SoQL on name that provides an implicit context. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName, text: String, parameters: Map[HoleName, ColumnType]): Result[FoundTables[MT]] = {
    val firstFrame = CallStack.Anonymous(None)
    walkFromName(ScopedResourceName(scope, resourceName), TableMap.empty, CallerStack.Implicit(firstFrame)).flatMap { acc =>
      walkSoQL(scope, FoundTables.InContext[MT](resourceName, _, _, parameters), text, acc, firstFrame)
    }
  }

  /** Find all tables referenced from the given SoQL on name that
    * provides an implicit context, impersonating a saved query.
    * The intent here is that we're editing that saved query, so
    * we want qualified parameter references in the edited SoQL
    * to be found, and we want that parameter map to be editable.
    */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName, text: String, parameters: Map[HoleName, ColumnType], impersonating: CanonicalName): Result[FoundTables[MT]] = {
    val firstFrame = CallStack.Anonymous(Some(impersonating))
    walkFromName(ScopedResourceName(scope, resourceName), TableMap.empty, CallerStack.Implicit(firstFrame)).flatMap { acc =>
      walkSoQL(scope, FoundTables.InContextImpersonatingSaved[MT](resourceName, _, _, parameters, impersonating), text, acc, firstFrame)
    }
  }

  /** Find all tables referenced from the given name. */
  final def findTables(scope: ResourceNameScope, resourceName: ResourceName): Result[FoundTables[MT]] = {
    walkFromName(ScopedResourceName(scope, resourceName), TableMap.empty, CallerStack.Empty).map { acc =>
      FoundTables[MT](acc, scope, FoundTables.Saved(resourceName), parserParameters)
    }
  }

  // A helper that lifts the abstract function into the Result world
  private def doLookup(
    scopedName: ScopedResourceName, // the name of the thing we're looking up
    caller: Option[Source[ResourceNameScope]] // the location of the reference of `scopedName`
  ): Result[TableDescription[MT]] = {
    lookup(scopedName) match {
      case Right(ds: Dataset) =>
        Right(ds.toParsed)
      case Right(Query(scope, canonicalName, basedOn, text, params, hiddenColumns, outputColumnHints)) =>
        parse(Some(scopedName), text, false).map(TableDescription.Query[MT](scope, canonicalName, basedOn, _, text, params, hiddenColumns, outputColumnHints))
      case Right(TableFunction(scope, canonicalName, text, params, hiddenColumns)) =>
        parse(Some(scopedName), text, true).map(TableDescription.TableFunction[MT](scope, canonicalName, _, text, params, hiddenColumns))
      case Left(LookupError.NotFound) =>
        Left(TableFinderError.NotFound(caller, scopedName.name))
      case Left(LookupError.PermissionDenied) =>
        Left(TableFinderError.PermissionDenied(caller, scopedName.name))
    }
  }

  private def parse(name: Option[ScopedResourceName], text: String, udfParamsAllowed: Boolean): Result[BinaryTree[ast.Select]] = {
    ParserUtil.parseInContext(name, text, parserParameters.copy(allowHoles = udfParamsAllowed))
  }

  private def walkFromName(scopedName: ScopedResourceName, acc: TableMap, callerStack: CallerStack): Result[TableMap] = {
    acc.get(scopedName) match {
      case Some(desc) =>
        callerStack.loopSource(desc.canonicalName) match {
          case Some(source) =>
            Left(TableFinderError.RecursiveQuery(source, desc.canonicalName +: callerStack.callstack))
          case None =>
            Right(acc)
        }
      case None =>
        if(Util.isSpecialTableName(scopedName)) {
          Right(acc)
        } else {
          doLookup(scopedName, callerStack.callerSource) match {
            case Right(desc) =>
              callerStack.loopSource(desc.canonicalName) match {
                case Some(source) =>
                  Left(TableFinderError.RecursiveQuery(source, desc.canonicalName +: callerStack.callstack))
                case None =>
                  val newStack = CallStack.Saved(scopedName, desc.canonicalName, callerStack)
                  walkDesc(scopedName, desc, acc + (scopedName -> desc), newStack)
              }
            case Left(err) =>
              Left(err)
          }
        }
    }
  }

  def walkDesc(scopedName: ScopedResourceName, desc: TableDescription[MT], acc: TableMap, stack: CallStack): Result[TableMap] = {
    desc match {
      case TableDescription.Dataset(_, _, _, _, _) => Right(acc)
      case TableDescription.Query(scope, canonicalName, basedOn, tree, _unparsed, _params, _hiddenColumns, _outputColumnHints) =>
        for {
          acc <- walkFromName(ScopedResourceName(scope, basedOn), acc, CallerStack.Implicit(stack))
          acc <- walkTree(scope, Some(scopedName), tree, acc, stack)
        } yield acc
      case TableDescription.TableFunction(scope, canonicalName, tree, _unparsed, _params, _hiddenColumns) =>
        walkTree(scope, Some(scopedName), tree, acc, stack)
    }
  }

  // This walks anonymous in a context soql.  Named soql gets parsed in doLookup
  private def walkSoQL(scope: ResourceNameScope, context: (BinaryTree[ast.Select], String) => FoundTables.Query[MT], text: String, acc: TableMap, stack: CallStack): Result[FoundTables[MT]] = {
    for {
      tree <- parse(None, text, udfParamsAllowed = false)
      acc <- walkTree(scope, None, tree, acc, stack)
    } yield {
      FoundTables[MT](acc, scope, context(tree, text), parserParameters)
    }
  }

  private def walkTree(scope: ResourceNameScope, treeName: Option[ScopedResourceName], bt: BinaryTree[ast.Select], acc: TableMap, stack: CallStack): Result[TableMap] = {
    bt match {
      case Leaf(s) => walkSelect(scope, treeName, s, acc, stack)
      case c: Compound[ast.Select] =>
        for {
          acc <- walkTree(scope, treeName, c.left, acc, stack)
          acc <- walkTree(scope, treeName, c.right, acc, stack)
        } yield acc
    }
  }

  private def walkSelect(scope: ResourceNameScope, selectName: Option[ScopedResourceName], s: ast.Select, acc0: TableMap, stack: CallStack): Result[TableMap] = {
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

    def effectiveSource(pos: Position) =
      Some(Source.nonSynthetic(selectName, pos))

    for(tn <- from) {
      val scopedName = ScopedResourceName(scope, ResourceName(tn.nameWithoutPrefix))
      walkFromName(scopedName, acc, CallerStack.Explicit(stack, NoPosition /* TODO: need position info from AST */)) match {
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
            walkFromName(ScopedResourceName(scope, ResourceName(tn.nameWithoutPrefix)), acc, CallerStack.Explicit(stack, NoPosition /* TODO: need position info from AST */))
          case ast.JoinQuery(q, _) =>
            walkTree(scope, selectName, q, acc, stack)
          case ast.JoinFunc(f, _) =>
            walkFromName(ScopedResourceName(scope, ResourceName(f.nameWithoutPrefix)), acc, CallerStack.Explicit(stack, NoPosition /* TODO: need position info from AST */))
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
