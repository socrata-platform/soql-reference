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

  def reduceMapRight[S, RNS2 >: RNS, CT2 >: CT, CV2 >: CV, RNS3, CT3, CV3](
    combine: (JoinType, Boolean, AtomicFrom[RNS2, CT2, CV2], From[RNS3, CT3, CV3], Expr[CT2, CV2], S) => (From[RNS3, CT3, CV3], S),
    base: AtomicFrom[RNS2, CT2, CV2] => (From[RNS3, CT3, CV3], S)
  ): (From[RNS3, CT3, CV3], S)

  final def mapRight[RNS2 >: RNS, CT2 >: CT, CV2 >: CV, RNS3, CT3, CV3](
    combine: (JoinType, Boolean, AtomicFrom[RNS2, CT2, CV2], From[RNS3, CT3, CV3], Expr[CT2, CV2]) => From[RNS3, CT3, CV3],
    base: AtomicFrom[RNS2, CT2, CV2] => From[RNS3, CT3, CV3]
  ): From[RNS3, CT3, CV3] = {
    reduceMapRight[Unit, RNS2, CT2, CV2, RNS3, CT3, CV3](
      { (joinType, lateral, left, right, on, _) => (combine(joinType, lateral, left, right, on), ()) },
      { (nonJoin: AtomicFrom[RNS2, CT2, CV2]) => (base.apply(nonJoin), ()) }
    )._1
  }

  final def reduceRight[RNS2 >: RNS, CT2 >: CT, CV2 >: CV, S](
    combine: (Join[RNS2, CT2, CV2], S) => S,
    base: AtomicFrom[RNS2, CT2, CV2] => S
  ): S =
    reduceMapRight[S, RNS2, CT2, CV2, RNS2, CT2, CV2](
      { (joinType, lateral, left, right, on, s) =>
        val j = Join(joinType, lateral, left, right, on)
        (j, combine(j, s))
      },
      { (nonJoin: AtomicFrom[RNS2, CT2, CV2]) => (nonJoin, base(nonJoin)) }
    )._2
}

case class Join[+RNS, +CT, +CV](joinType: JoinType, lateral: Boolean, left: AtomicFrom[RNS, CT, CV], right: From[RNS, CT, CV], on: Expr[CT, CV]) extends From[RNS, CT, CV] {
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
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]) = {
    @tailrec
    def loop(acc: Environment[CT2], self: From[RNS, CT2, CV]): Either[AddScopeError, Environment[CT2]] = {
      self match {
        case j@Join(_, _, left, right, _) =>
          acc.addScope(left.alias.map(_._2), left.scope) match {
            case Right(env) => loop(env, right)
            case Left(err) => Left(err)
          }
        case other: AtomicFrom[RNS, CT2, CV] =>
          other.addToEnvironment(acc)
      }
    }
    loop(base.extend, this)
  }

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName] = {
    @tailrec
    def loop(acc: Map[AutoTableLabel, DatabaseTableName], self: From[RNS, CT, CV]): Map[AutoTableLabel, DatabaseTableName] = {
      self match {
        case j@Join(_, _, left, right, _) =>
          loop(acc ++ left.realTables, right)
        case other =>
          acc ++ other.realTables
      }
    }
    loop(Map.empty, this)
  }

  override def reduceMapRight[S, RNS2 >: RNS, CT2 >: CT, CV2 >: CV, RNS3, CT3, CV3](
    combine: (JoinType, Boolean, AtomicFrom[RNS2, CT2, CV2], From[RNS3, CT3, CV3], Expr[CT2, CV2], S) => (From[RNS3, CT3, CV3], S),
    base: AtomicFrom[RNS2, CT2, CV2] => (From[RNS3, CT3, CV3], S)
  ): (From[RNS3, CT3, CV3], S) = {
    type Stack = List[(From[RNS3, CT3, CV3], S) => (From[RNS3, CT3, CV3], S)]

    @tailrec
    def loop(self: From[RNS2, CT2, CV2], stack: Stack): (From[RNS3, CT3, CV3], S) = {
      self match {
        case Join(joinType, lateral, left, right, on) =>
          loop(right, { (newRight, s) => combine(joinType, lateral, left, newRight, on, s) } :: stack)
        case last: AtomicFrom[RNS2, CT2, CV2] =>
          stack.foldLeft(base(last)) { (acc, f) =>
            val (right, s) = acc
            f(right, s)
          }
      }
    }

    loop(this, Nil)
  }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Join[RNS, CT, CV] = {
    mapRight[RNS, CT, CV, RNS, CT, CV](
       { (joinType, lateral, left, right, on) =>
         val newLeft = left.doRewriteDatabaseNames(state)
         val newOn = on.doRewriteDatabaseNames(state)
         Join(joinType, lateral, newLeft, right, newOn)
       },
       _.doRewriteDatabaseNames(state)
    ).asInstanceOf[Join[RNS, CT, CV]]
  }

  private[analyzer2] def doRelabel(state: RelabelState): Join[RNS, CT, CV] = {
    mapRight[RNS, CT, CV, RNS, CT, CV](
      { (joinType, lateral, left, right, on) =>
        val newLeft = left.doRelabel(state)
        val newOn = on.doRelabel(state)
        Join(joinType, lateral, newLeft, right, newOn)
      },
      _.doRelabel(state)
    ).asInstanceOf[Join[RNS, CT, CV]]
  }

  def useSelectListReferences: Join[RNS, CT, CV] = {
    mapRight[RNS, CT, CV, RNS, CT, CV](
      { (joinType, lateral, left, right, on) =>
        val newLeft = left.useSelectListReferences
        Join(joinType, lateral, newLeft, right, on)
      },
      _.useSelectListReferences
    ).asInstanceOf[Join[RNS, CT, CV]]
  }

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV] =
    mapRight[RNS, CT, CV, RNS2, CT, CV](
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left.mapAlias(f), right, on) },
      _.mapAlias(f)
    ).asInstanceOf[Join[RNS2, CT, CV]]

  private[analyzer2] override def preserveOrdering[CT2 >: CT](
    provider: LabelProvider,
    rowNumberFunction: MonomorphicFunction[CT2],
    wantOutputOrdered: Boolean,
    wantOrderingColumn: Boolean
  ): (Option[(TableLabel, AutoColumnLabel)], Self[RNS, CT2, CV]) = {
    // JOIN builds a new table, which is unordered (hence false, false)
    val result = mapRight[RNS, CT, CV, RNS, CT2, CV](
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left.preserveOrdering(provider, rowNumberFunction, false, false)._2, right, on) },
      { _.preserveOrdering(provider, rowNumberFunction, false, false)._2 }
    )
    (None, result.asInstanceOf[Join[RNS, CT2, CV]])
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
    reduceRight[RNS, CT, CV, Option[Expr[CT, CV]] => Doc[Annotation[RNS, CT]]](
      { (j, rightDoc) => lastOn: Option[Expr[CT, CV]] =>
        val Join(joinType, lateral, left, right, on) = j
        Seq(
          Seq(
            Some(left.debugDoc),
            lastOn.map { e => Seq(d"ON", e.debugDoc).sep.nest(2) }
          ).flatten.sep.nest(2),
          Seq(
            Some(joinType.debugDoc),
            if(lateral) Some(d"LATERAL") else None,
            Some(rightDoc(Some(on)))
          ).flatten.hsep
        ).sep
      },
      { lastJoin => lastOn: Option[Expr[CT, CV]] =>
        Seq(
          Some(lastJoin.debugDoc),
          lastOn.map { e => Seq(d"ON", e.debugDoc).sep.nest(2) }
        ).flatten.sep.nest(2)
      }
    )(None)
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

  override final def reduceMapRight[S, RNS2 >: RNS, CT2 >: CT, CV2 >: CV, RNS3, CT3, CV3](
    combine: (JoinType, Boolean, AtomicFrom[RNS2, CT2, CV2], From[RNS3, CT3, CV3], Expr[CT2, CV2], S) => (From[RNS3, CT3, CV3], S),
    base: AtomicFrom[RNS2, CT2, CV2] => (From[RNS3, CT3, CV3], S)
  ): (From[RNS3, CT3, CV3], S) =
    base(this)

  private[analyzer2] def reAlias[RNS2 >: RNS](newAlias: Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV]
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
