package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ResourceName

sealed abstract class From[+RNS, +CT, +CV] {
  type Self[+RNS, +CT, +CV] <: From[RNS, CT, CV]

  // extend the given environment with names introduced by this FROM clause
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]): Either[AddScopeError, Environment[CT2]]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Self[RNS, CT, CV]

  private[analyzer2] def doRelabel(state: RelabelState): Self[RNS, CT, CV]

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName]

  def numericate: Self[RNS, CT, CV]

  def debugStr(sb: StringBuilder): StringBuilder

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
}

case class Join[+RNS, +CT, +CV](joinType: JoinType, lateral: Boolean, left: AtomicFrom[RNS, CT, CV], right: From[RNS, CT, CV], on: Expr[CT, CV]) extends From[RNS, CT, CV] {
  type Self[+RNS, +CT, +CV] = Join[RNS, CT, CV]

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

  def numericate: Join[RNS, CT, CV] = {
    mapRight[RNS, CT, CV, RNS, CT, CV](
      { (joinType, lateral, left, right, on) =>
        val newLeft = left.numericate
        Join(joinType, lateral, newLeft, right, on)
      },
      _.numericate
    ).asInstanceOf[Join[RNS, CT, CV]]
  }

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV] =
    mapRight[RNS, CT, CV, RNS2, CT, CV](
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left.mapAlias(f), right, on) },
      _.mapAlias(f)
    ).asInstanceOf[Join[RNS2, CT, CV]]

  def debugStr(sb: StringBuilder): StringBuilder = {
    left.debugStr(sb)
    def loop(prevJoin: Join[RNS, CT, CV], from: From[RNS, CT, CV]): StringBuilder = {
      from match {
        case j: Join[RNS, CT, CV] =>
          sb.
            append(' ').
            append(prevJoin.joinType).
            append(if(prevJoin.lateral) " LATERAL" else "").
            append(' ')
          j.left.debugStr(sb).
            append(" ON ")
          prevJoin.on.debugStr(sb)
          loop(j, j.right)
        case nonJoin =>
          sb.append(' ').
            append(prevJoin.joinType).
            append(if(prevJoin.lateral) " LATERAL" else "").
            append(' ')
          nonJoin.debugStr(sb).
            append(" ON ")
          prevJoin.on.debugStr(sb)
      }
    }
    loop(this, this.right)
  }
}

sealed abstract class JoinType
object JoinType {
  case object Inner extends JoinType
  case object LeftOuter extends JoinType
  case object RightOuter extends JoinType
  case object FullOuter extends JoinType
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
  val columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]

  private[analyzer2] override final val scope: Scope[CT] = Scope(columns, label)

  override final def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append(tableName).append(" AS ").append(label)
  }
}

case class FromTable[+RNS, +CT](tableName: DatabaseTableName, alias: Option[(RNS, ResourceName)], label: AutoTableLabel, columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]) extends FromTableLike[RNS, CT] {
  type Self[+RNS, +CT, +CV] = FromTable[RNS, CT]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      tableName = state.convert(this.tableName),
      columns = OrderedMap() ++ columns.iterator.map { case (n, ne) => state.convert(this.tableName, n) -> ne }
    )

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  private[analyzer2] def realTables = Map(label -> tableName)

  private[analyzer2] def reAlias[RNS2](newAlias: Option[(RNS2, ResourceName)]): FromTable[RNS2, CT] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, Nothing] =
    copy(alias = f(alias))

  def numericate: this.type = this
}

case class FromVirtualTable[+RNS, +CT](tableName: AutoTableLabel, alias: Option[(RNS, ResourceName)], label: AutoTableLabel, columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]) extends FromTableLike[RNS, CT] {
  // This is just like FromTable except it does not participate in the
  // DatabaseName-renaming system.

  type Self[+RNS, +CT, +CV] = FromVirtualTable[RNS, CT]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(tableName = state.convert(tableName), label = state.convert(label))
  }

  private[analyzer2] def realTables = Map.empty

  private[analyzer2] def reAlias[RNS2](newAlias: Option[(RNS2, ResourceName)]): FromVirtualTable[RNS2, CT] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, Nothing] =
    copy(alias = f(alias))

  def numericate: this.type = this
}

// "alias" is optional here because of chained soql; actually having a
// real subselect syntactically requires an alias, but `select ... |>
// select ...` does not.  The alias is just for name-resolution during
// analysis anyway...
case class FromStatement[+RNS, +CT, +CV](statement: Statement[RNS, CT, CV], label: TableLabel, alias: Option[(RNS, ResourceName)]) extends AtomicFrom[RNS, CT, CV] {
  type Self[+RNS, +CT, +CV] = FromStatement[RNS, CT, CV]

  private[analyzer2] val scope: Scope[CT] = Scope(statement.schema, label)

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(statement = statement.doRewriteDatabaseNames(state))

  def numericate = copy(statement = statement.numericate)

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(statement = statement.doRelabel(state),
         label = state.convert(label))
  }

  private[analyzer2] def reAlias[RNS2 >: RNS](newAlias: Option[(RNS2, ResourceName)]): FromStatement[RNS2, CT, CV] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, CT, CV] =
    copy(statement = statement.mapAlias(f), alias = f(alias))

  private[analyzer2] def realTables = Map.empty

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append('(')
    statement.debugStr(sb)
    sb.append(") AS ").append(label)
  }
}

case class FromSingleRow[+RNS](label: TableLabel, alias: Option[(RNS, ResourceName)]) extends AtomicFrom[RNS, Nothing, Nothing] {
  type Self[+RNS, +CT, +CV] = FromSingleRow[RNS]

  private[analyzer2] val scope: Scope[Nothing] =
    Scope(
      OrderedMap.empty[ColumnLabel, NameEntry[Nothing]],
      label
    )

  def numericate = this

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  private[analyzer2] def reAlias[RNS2 >: RNS](newAlias: Option[(RNS2, ResourceName)]): FromSingleRow[RNS2] =
    copy(alias = newAlias)

  def mapAlias[RNS2](f: Option[(RNS, ResourceName)] => Option[(RNS2, ResourceName)]): Self[RNS2, Nothing, Nothing] =
    copy(alias = f(alias))

  private[analyzer2] def realTables = Map.empty

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append("@single_row")
  }
}
