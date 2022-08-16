package com.socrata.soql.analyzer2

import scala.language.higherKinds
import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

import DocUtils._

sealed abstract class From[+RNS, +CT, +CV] {
  type Self[+RNS, +CT, +CV] <: From[RNS, CT, CV]
  def asSelf: Self[RNS, CT, CV]

  // extend the given environment with names introduced by this FROM clause
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]): Either[AddScopeError, Environment[CT2]]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Self[RNS, CT, CV]

  private[analyzer2] def doRelabel(state: RelabelState): Self[RNS, CT, CV]

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName]

  private[analyzer2] def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean

  private[analyzer2] def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, CT2, CV])

  def useSelectListReferences: Self[RNS, CT, CV]

  final def debugStr(implicit ev: HasDoc[CV]): String = debugStr(new StringBuilder).toString
  final def debugStr(sb: StringBuilder)(implicit ev: HasDoc[CV]): StringBuilder = debugDoc.layoutSmart().toStringBuilder(sb)
  def debugDoc(implicit ev: HasDoc[CV]): Doc[Annotation[RNS, CT]]

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV]

  type ReduceResult[+RNS, +CT, +CV] <: From[RNS, CT, CV]

  def reduceMap[S, RNS2, CT2, CV2](
    base: AtomicFrom[RNS, CT, CV] => (S, AtomicFrom[RNS2, CT2, CV2]),
    combine: (S, JoinType, Boolean, From[RNS2, CT2, CV2], AtomicFrom[RNS, CT, CV], Expr[CT, CV]) => (S, Join[RNS2, CT2, CV2])
  ): (S, ReduceResult[RNS2, CT2, CV2])

  final def map[RNS2, CT2, CV2](
    base: AtomicFrom[RNS, CT, CV] => AtomicFrom[RNS2, CT2, CV2],
    combine: (JoinType, Boolean, From[RNS2, CT2, CV2], AtomicFrom[RNS, CT, CV], Expr[CT, CV]) => Join[RNS2, CT2, CV2]
  ): ReduceResult[RNS2, CT2, CV2] = {
    reduceMap[Unit, RNS2, CT2, CV2](
      { (nonJoin: AtomicFrom[RNS, CT, CV]) => ((), base.apply(nonJoin)) },
      { (_, joinType, lateral, left, right, on) => ((), combine(joinType, lateral, left, right, on)) }
    )._2
  }

  final def reduce[S](
    base: AtomicFrom[RNS, CT, CV] => S,
    combine: (S, Join[RNS, CT, CV]) => S
  ): S =
    reduceMap[S, RNS, CT, CV](
      { (nonJoin: AtomicFrom[RNS, CT, CV]) => (base(nonJoin), nonJoin) },
      { (s, joinType, lateral, left, right, on) =>
        val j = Join(joinType, lateral, left, right, on)
        (combine(s, j), j)
      }
    )._1
}

case class Join[+RNS, +CT, +CV](joinType: JoinType, lateral: Boolean, left: From[RNS, CT, CV], right: AtomicFrom[RNS, CT, CV], on: Expr[CT, CV]) extends From[RNS, CT, CV] {
  type Self[+RNS, +CT, +CV] = Join[RNS, CT, CV]
  def asSelf = this

  // The difference between a lateral and a non-lateral join is the
  // environment assumed while typechecking; in a non-lateral join
  // it's something like:
  //    val checkedLeft = left.typeCheckIn(enclosingEnv)
  //    val checkedRight = right.typeCheckIn(enclosingEnv)
  // whereas in a lateral join it's like
  //    val checkedLeft = left.typecheckIn(enclosingEnv)
  //    val checkedRight = right.typecheckIn(checkedLeft.extendEnvironment(previousFromEnv))
  // In both cases the "next" FROM env (where "on" is typechecked) is
  //    val nextFromEnv = checkedRight.extendEnvironment(checkedLeft.extendEnvironment(previousFromEnv))
  // which is what this `extendEnvironment` function does, rewritten
  // as a loop so that a lot of joins don't use a lot of stack.
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]): Either[AddScopeError, Environment[CT2]] = {
    type Stack = List[Environment[CT2] => Either[AddScopeError, Environment[CT2]]]

    @tailrec
    def loop(stack: Stack, self: From[RNS, CT, CV]): (Stack, AtomicFrom[RNS, CT, CV]) = {
      self match {
        case j: Join[RNS, CT, CV] =>
          loop(right.addToEnvironment[CT2] _ :: stack, j.left)
        case other: AtomicFrom[RNS, CT, CV] =>
          (stack, other)
      }
    }

    var (stack, leftmost) = loop(Nil, this)
    leftmost.extendEnvironment(base) match {
      case Right(e0) =>
        var env = e0
        while(!stack.isEmpty) {
          stack.head(env) match {
            case Left(err) => return Left(err)
            case Right(e) => env = e
          }
          stack = stack.tail
        }
        Right(env)
      case Left(err) =>
        Left(err)
    }
  }

  type ReduceResult[+RNS, +CT, +CV] = Join[RNS, CT, CV]

  override def reduceMap[S, RNS2, CT2, CV2](
    base: AtomicFrom[RNS, CT, CV] => (S, AtomicFrom[RNS2, CT2, CV2]),
    combine: (S, JoinType, Boolean, From[RNS2, CT2, CV2], AtomicFrom[RNS, CT, CV], Expr[CT, CV]) => (S, Join[RNS2, CT2, CV2])
  ): (S, ReduceResult[RNS2, CT2, CV2]) = {
    type Stack = List[(S, From[RNS2, CT2, CV2]) => (S, Join[RNS2, CT2, CV2])]

    @tailrec
    def loop(self: From[RNS, CT, CV], stack: Stack): (S, From[RNS2, CT2, CV2]) = {
      self match {
        case Join(joinType, lateral, left, right, on) =>
          loop(left, { (s, newLeft) => combine(s, joinType, lateral, newLeft, right, on) } :: stack)
        case leftmost: AtomicFrom[RNS, CT, CV] =>
          stack.foldLeft[(S, From[RNS2, CT2, CV2])](base(leftmost)) { (acc, f) =>
            val (s, left) = acc
            f(s, left)
          }
      }
    }

    loop(this, Nil).asInstanceOf[(S, Join[RNS2, CT2, CV2])]
  }

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName] = {
    reduce[Map[AutoTableLabel, DatabaseTableName]] (
      { other => other.realTables },
      { (acc, j) => acc ++ j.right.realTables }
    )
  }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Join[RNS, CT, CV] = {
    map[RNS, CT, CV](
       _.doRewriteDatabaseNames(state),
       { (joinType, lateral, left, right, on) =>
         val newRight = right.doRewriteDatabaseNames(state)
         val newOn = on.doRewriteDatabaseNames(state)
         Join(joinType, lateral, left, newRight, newOn)
       }
    )
  }

  private[analyzer2] def doRelabel(state: RelabelState): Join[RNS, CT, CV] = {
    map[RNS, CT, CV](
      _.doRelabel(state),
      { (joinType, lateral, left, right, on) =>
        val newRight = right.doRelabel(state)
        val newOn = on.doRelabel(state)
        Join(joinType, lateral, left, newRight, newOn)
      }
    )
  }

  def useSelectListReferences: Join[RNS, CT, CV] = {
    map[RNS, CT, CV](
      _.useSelectListReferences,
      { (joinType, lateral, left, right, on) =>
        val newRight = right.useSelectListReferences
        Join(joinType, lateral, left, newRight, on)
      }
    )
  }

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV] =
    map[RNS2, CT, CV](
      _.mapAlias(f),
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, right.mapAlias(f), on) },
    )

  private[analyzer2] override def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, CT2, CV]) = {
    // JOIN builds a new table, which is unordered (hence false, false)
    val result = map[RNS, CT2, CV](
      { _.preserveOrdering(provider, rowNumberFunction, false, false)._2 },
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, right.preserveOrdering(provider, rowNumberFunction, false, false)._2, on) },
    )
    (None, result)
  }

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2 >: CV](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case Join(thatJoinType, thatLateral, thatLeft, thatRight, thatOn) =>
        this.joinType == thatJoinType &&
          this.lateral == thatLateral &&
          this.left.findIsomorphism(state, thatLeft) &&
          this.right.findIsomorphism(state, thatRight) &&
          this.on.findIsomorphism(state, thatOn)
      case _ =>
        false
    }

  def debugDoc(implicit ev: HasDoc[CV]) =
    reduce[Doc[Annotation[RNS, CT]]](
      _.debugDoc,
      { (leftDoc, j) =>
        val Join(joinType, lateral, _left, right, on) = j
        Seq(
          leftDoc,
          Seq(
            Some(joinType.debugDoc),
            if(lateral) Some(d"LATERAL") else None,
            Some(right.debugDoc),
            Some(d"ON"),
            Some(on.debugDoc)
          ).flatten.hsep
        ).sep
      },
    )
}

sealed abstract class JoinType(val debugDoc: Doc[Nothing])
object JoinType {
  case object Inner extends JoinType(d"JOIN")
  case object LeftOuter extends JoinType(d"LEFT OUTER JOIN")
  case object RightOuter extends JoinType(d"RIGHT OUTER JOIN")
  case object FullOuter extends JoinType(d"FULL OUTER JOIN")
}

sealed abstract class AtomicFrom[+RNS, +CT, +CV] extends From[RNS, CT, CV] {
  type Self[+RNS, +CT, +CV] <: AtomicFrom[RNS, CT, CV]

  val alias: Option[(RNS, ResourceName)]
  val label: TableLabel

  private[analyzer2] val scope: Scope[CT]

  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]) = {
    addToEnvironment(base.extend)
  }
  private[analyzer2] def addToEnvironment[CT2 >: CT](env: Environment[CT2]) = {
    env.addScope(alias.map(_._2), scope)
  }

  private[analyzer2] def reAlias[RNS2 >: RNS](newAlias: Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV]

  type ReduceResult[+RNS, +CT, +CV] = AtomicFrom[RNS, CT, CV]
  override def reduceMap[S, RNS2, CT2, CV2](
    base: AtomicFrom[RNS, CT, CV] => (S, AtomicFrom[RNS2, CT2, CV2]),
    combine: (S, JoinType, Boolean, From[RNS2, CT2, CV2], AtomicFrom[RNS, CT, CV], Expr[CT, CV]) => (S, Join[RNS2, CT2, CV2])
  ): (S, ReduceResult[RNS2, CT2, CV2]) =
    base(this)
}

sealed abstract class FromTableLike[+RNS, +CT] extends AtomicFrom[RNS, CT, Nothing] {
  type Self[+RNS, +CT, +CV] <: FromTableLike[RNS, CT]

  val tableName: TableLabel
  val label: TableLabel
  val columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]

  private[analyzer2] override final val scope: Scope[CT] = Scope(columns, label)

  private[analyzer2] override def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, CT2, Nothing]) =
    (None, asSelf)

  def debugDoc(implicit ev: HasDoc[Nothing]) =
    (tableName.debugDoc ++ Doc.softlineSep ++ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))
}

case class FromTable[+RNS, +CT](tableName: DatabaseTableName, alias: Option[(RNS, ResourceName)], label: AutoTableLabel, columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]) extends FromTableLike[RNS, CT] {
  type Self[+RNS, +CT, +CV] = FromTable[RNS, CT]
  def asSelf = this

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      tableName = state.convert(this.tableName),
      columns = OrderedMap() ++ columns.iterator.map { case (n, ne) => state.convert(this.tableName, n) -> ne }
    )

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case FromTable(thatTableName, thatAlias, thatLabel, thatColumns) =>
        this.tableName == thatTableName &&
          // don't care about aliases
          state.tryAssociate(this.label, thatLabel) &&
          this.columns.size == thatColumns.size &&
          this.columns.iterator.zip(thatColumns.iterator).forall { case ((thisColName, thisEntry), (thatColName, thatEntry)) =>
            thisColName == thatColName &&
              thisEntry.typ == thatEntry.typ
            // don't care about the entry's name
          }
      case _ =>
        false
    }

  private[analyzer2] def realTables = Map(label -> tableName)

  private[analyzer2] def reAlias[RNS2](newAlias: Option[(RNS2, ResourceName)]): FromTable[RNS2, CT] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, Nothing] =
    copy(alias = f(alias))

  def useSelectListReferences: this.type = this
}

case class FromVirtualTable[+RNS, +CT](tableName: AutoTableLabel, alias: Option[(RNS, ResourceName)], label: AutoTableLabel, columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]) extends FromTableLike[RNS, CT] {
  // This is just like FromTable except it does not participate in the
  // DatabaseName-renaming system.

  type Self[+RNS, +CT, +CV] = FromVirtualTable[RNS, CT]
  def asSelf = this

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(tableName = state.convert(tableName), label = state.convert(label))
  }

  private[analyzer2] def realTables = Map.empty

  private[analyzer2] def reAlias[RNS2](newAlias: Option[(RNS2, ResourceName)]): FromVirtualTable[RNS2, CT] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, Nothing] =
    copy(alias = f(alias))

  def useSelectListReferences: this.type = this

  private[analyzer2] override def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, CT2, Nothing]) =
    (None, this)

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case FromTable(thatTableName, thatAlias, thatLabel, thatColumns) =>
        this.tableName == thatTableName &&
          // don't care about aliases
          state.tryAssociate(this.label, thatLabel) &&
          this.columns.size == thatColumns.size &&
          this.columns.iterator.zip(thatColumns.iterator).forall { case ((thisColName, thisEntry), (thatColName, thatEntry)) =>
            thisColName == thatColName &&
              thisEntry.typ == thatEntry.typ
            // don't care about the entry's name
          }
      case _ =>
        false
    }
}

// "alias" is optional here because of chained soql; actually having a
// real subselect syntactically requires an alias, but `select ... |>
// select ...` does not.  The alias is just for name-resolution during
// analysis anyway...
case class FromStatement[+RNS, +CT, +CV](statement: Statement[RNS, CT, CV], label: TableLabel, alias: Option[(RNS, ResourceName)]) extends AtomicFrom[RNS, CT, CV] {
  type Self[+RNS, +CT, +CV] = FromStatement[RNS, CT, CV]
  def asSelf = this

  private[analyzer2] val scope: Scope[CT] = Scope(statement.schema, label)

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2 >: CT, CV2](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
    // TODO: make this constant-stack if it ever gets used outside of tests
    that match {
      case FromStatement(thatStatement, thatLabel, thatAlias) =>
        state.tryAssociate(this.label, thatLabel) &&
          this.statement.findIsomorphism(state, Some(this.label), Some(thatLabel), thatStatement)
        // don't care about aliases
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(statement = statement.doRewriteDatabaseNames(state))

  def useSelectListReferences = copy(statement = statement.useSelectListReferences)

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(statement = statement.doRelabel(state),
         label = state.convert(label))
  }

  private[analyzer2] def reAlias[RNS2 >: RNS](newAlias: Option[(RNS2, ResourceName)]): FromStatement[RNS2, CT, CV] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV] =
    copy(statement = statement.mapAlias(f), alias = f(alias))

  private[analyzer2] def realTables = Map.empty

  private[analyzer2] override def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, CT2, CV]) = {
    val (orderColumn, stmt) =
      statement.preserveOrdering(provider, rowNumberFunction, wantOutputOrdered, wantOrderingColumn)

    (orderColumn.map((label, _)), copy(statement = stmt))
  }

  def debugDoc(implicit ev: HasDoc[CV]) =
    (statement.debugDoc.encloseNesting(d"(", d")") +#+ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))
}

case class FromSingleRow[+RNS](label: TableLabel, alias: Option[(RNS, ResourceName)]) extends AtomicFrom[RNS, Nothing, Nothing] {
  type Self[+RNS, +CT, +CV] = FromSingleRow[RNS]
  def asSelf = this

  private[analyzer2] val scope: Scope[Nothing] =
    Scope(
      OrderedMap.empty[ColumnLabel, NameEntry[Nothing]],
      label
    )

  def useSelectListReferences = this

  private[analyzer2] final def findIsomorphism[RNS2 >: RNS, CT2, CV2](state: IsomorphismState, that: From[RNS2, CT2, CV2]): Boolean =
    that match {
      case FromSingleRow(thatLabel, thatAlias) =>
        state.tryAssociate(this.label, thatLabel)
        // don't care about aliases
      case _ =>
        false
    }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  private[analyzer2] def reAlias[RNS2 >: RNS](newAlias: Option[(RNS2, ResourceName)]): FromSingleRow[RNS2] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, Nothing, Nothing] =
    copy(alias = f(alias))

  private[analyzer2] def realTables = Map.empty

  private[analyzer2] override def preserveOrdering[CT2](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, Nothing, Nothing]) =
    (None, this)

  def debugDoc(implicit ev: HasDoc[Nothing]) =
    (d"(SELECT)" +#+ d"AS" +#+ label.debugDoc.annotate(Annotation.TableAliasDefinition(alias, label))).annotate(Annotation.TableDefinition(label))
}
