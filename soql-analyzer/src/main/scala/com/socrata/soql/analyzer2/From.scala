package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ResourceName

sealed abstract class From[+CT, +CV] {
  type Self[+CT, +CV] <: From[CT, CV]

  // extend the given environment with names introduced by this FROM clause
  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]): Either[AddScopeError, Environment[CT2]]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Self[CT, CV]

  private[analyzer2] def doRelabel(state: RelabelState): Self[CT, CV]

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName]

  def numericate: Self[CT, CV]

  def debugStr(sb: StringBuilder): StringBuilder

  def reduceMapRight[S, CT2 >: CT, CV2 >: CV, CT3, CV3](combine: (JoinType, Boolean, AtomicFrom[CT2, CV2], From[CT3, CV3], Expr[CT2, CV2], S) => (Join[CT3, CV3], S), base: AtomicFrom[CT2, CV2] => (From[CT3, CV3], S)): (From[CT3, CV3], S)
  def mapRight[CT2 >: CT, CV2 >: CV, CT3, CV3](combine: (JoinType, Boolean, AtomicFrom[CT2, CV2], From[CT3, CV3], Expr[CT2, CV2]) => Join[CT3, CV3], base: AtomicFrom[CT2, CV2] => From[CT3, CV3]): From[CT3, CV3]
}

case class Join[+CT, +CV](joinType: JoinType, lateral: Boolean, left: AtomicFrom[CT, CV], right: From[CT, CV], on: Expr[CT, CV]) extends From[CT, CV] {
  type Self[+CT, +CV] = Join[CT, CV]

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
    def loop(acc: Environment[CT2], self: From[CT2, CV]): Either[AddScopeError, Environment[CT2]] = {
      self match {
        case j@Join(_, _, left, right, _) =>
          acc.addScope(left.alias, left.scope) match {
            case Right(env) => loop(env, right)
            case Left(err) => Left(err)
          }
        case other: AtomicFrom[CT2, CV] =>
          other.addToEnvironment(acc)
      }
    }
    loop(base.extend, this)
  }

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName] = {
    @tailrec
    def loop(acc: Map[AutoTableLabel, DatabaseTableName], self: From[CT, CV]): Map[AutoTableLabel, DatabaseTableName] = {
      self match {
        case j@Join(_, _, left, right, _) =>
          loop(acc ++ left.realTables, right)
        case other =>
          acc ++ other.realTables
      }
    }
    loop(Map.empty, this)
  }

  override def reduceMapRight[S, CT2 >: CT, CV2 >: CV, CT3, CV3](combine: (JoinType, Boolean, AtomicFrom[CT2, CV2], From[CT3, CV3], Expr[CT2, CV2], S) => (Join[CT3, CV3], S), base: AtomicFrom[CT2, CV2] => (From[CT3, CV3], S)): (Join[CT3, CV3], S) = {
    type Stack = List[(From[CT3, CV3], S) => (Join[CT3, CV3], S)]

    @tailrec
    def loop(self: From[CT2, CV2], stack: Stack): (From[CT3, CV3], S) = {
      self match {
        case Join(joinType, lateral, left, right, on) =>
          loop(right, { (newRight, s) => combine(joinType, lateral, left, newRight, on, s) } :: stack)
        case last: AtomicFrom[CT2, CV2] =>
          stack.foldLeft(base(last)) { (acc, f) =>
            val (right, s) = acc
            f(right, s)
          }
      }
    }

    val (result, s) = loop(this, Nil)

    (result.asInstanceOf[Join[CT3, CV3]], s)
  }

  override def mapRight[CT2 >: CT, CV2 >: CV, CT3, CV3](combine: (JoinType, Boolean, AtomicFrom[CT2, CV2], From[CT3, CV3], Expr[CT2, CV2]) => Join[CT3, CV3], base: AtomicFrom[CT2, CV2] => From[CT3, CV3]): Join[CT3, CV3] = {
    reduceMapRight[Unit, CT2, CV2, CT3, CV3](
      { (joinType, lateral, left, right, on, _) => (combine(joinType, lateral, left, right, on), ()) },
      { nonJoin => (base(nonJoin), ()) }
    )._1
  }

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState): Join[CT, CV] = {
    mapRight[CT, CV, CT, CV](
       { (joinType, lateral, left, right, on) =>
         val newLeft = left.doRewriteDatabaseNames(state)
         val newOn = on.doRewriteDatabaseNames(state)
         Join(joinType, lateral, newLeft, right, newOn)
       },
       _.doRewriteDatabaseNames(state)
    )
  }

  private[analyzer2] def doRelabel(state: RelabelState): Join[CT, CV] = {
    mapRight[CT, CV, CT, CV](
      { (joinType, lateral, left, right, on) =>
        val newLeft = left.doRelabel(state)
        val newOn = on.doRelabel(state)
        Join(joinType, lateral, newLeft, right, newOn)
      },
      _.doRelabel(state)
    )
  }

  def numericate: Join[CT, CV] = {
    mapRight[CT, CV, CT, CV](
      { (joinType, lateral, left, right, on) =>
        val newLeft = left.numericate
        Join(joinType, lateral, newLeft, right, on)
      },
      _.numericate
    )
  }

  def debugStr(sb: StringBuilder): StringBuilder = {
    left.debugStr(sb)
    def loop(prevJoin: Join[CT, CV], from: From[CT, CV]): StringBuilder = {
      from match {
        case j: Join[CT, CV] =>
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

sealed abstract class AtomicFrom[+CT, +CV] extends From[CT, CV] {
  type Self[+CT, +CV] <: AtomicFrom[CT, CV]

  val alias: Option[ResourceName]
  val label: TableLabel

  private[analyzer2] val scope: Scope[CT]

  private[analyzer2] def extendEnvironment[CT2 >: CT](base: Environment[CT2]) = {
    addToEnvironment(base.extend)
  }
  private[analyzer2] def addToEnvironment[CT2 >: CT](env: Environment[CT2]) = {
    env.addScope(alias, scope)
  }

  override final def reduceMapRight[S, CT2 >: CT, CV2 >: CV, CT3, CV3](combine: (JoinType, Boolean, AtomicFrom[CT2, CV2], From[CT3, CV3], Expr[CT2, CV2], S) => (Join[CT3, CV3], S), base: AtomicFrom[CT2, CV2] => (From[CT3, CV3], S)): (From[CT3, CV3], S) =
    base(this)

  override final def mapRight[CT2 >: CT, CV2 >: CV, CT3, CV3](combine: (JoinType, Boolean, AtomicFrom[CT2, CV2], From[CT3, CV3], Expr[CT2, CV2]) => Join[CT3, CV3], base: AtomicFrom[CT2, CV2] => From[CT3, CV3]): From[CT3, CV3] =
    base(this)

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): Self[CT, CV]
}

sealed abstract class FromTableLike[+CT] extends AtomicFrom[CT, Nothing] {
  type Self[+CT, +CV] <: FromTableLike[CT]

  val tableName: TableLabel
  val columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]

  private[analyzer2] override final val scope: Scope[CT] = Scope(columns, label)

  override final def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append(tableName).append(" AS ").append(label)
  }
}

case class FromTable[+CT](tableName: DatabaseTableName, alias: Option[ResourceName], label: AutoTableLabel, columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]) extends FromTableLike[CT] {
  type Self[+CT, +CV] = FromTable[CT]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(
      tableName = state.convert(this.tableName),
      columns = OrderedMap() ++ columns.iterator.map { case (n, ne) => state.convert(this.tableName, n) -> ne }
    )

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(label = state.convert(label))
  }

  private[analyzer2] def realTables = Map(label -> tableName)

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromTable[CT] =
    copy(alias = newAlias)


  def numericate: this.type = this
}

case class FromVirtualTable[+CT](tableName: AutoTableLabel, alias: Option[ResourceName], label: AutoTableLabel, columns: OrderedMap[DatabaseColumnName, NameEntry[CT]]) extends FromTableLike[CT] {
  // This is just like FromTable except it does not participate in the
  // DatabaseName-renaming system.

  type Self[+CT, +CV] = FromVirtualTable[CT]

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) = this

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(tableName = state.convert(tableName), label = state.convert(label))
  }

  private[analyzer2] def realTables = Map.empty

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromVirtualTable[CT] =
    copy(alias = newAlias)

  def numericate: this.type = this
}

// "alias" is optional here because of chained soql; actually having a
// real subselect syntactically requires an alias, but `select ... |>
// select ...` does not.  The alias is just for name-resolution during
// analysis anyway...
case class FromStatement[+CT, +CV](statement: Statement[CT, CV], label: TableLabel, alias: Option[ResourceName]) extends AtomicFrom[CT, CV] {
  type Self[+CT, +CV] = FromStatement[CT, CV]

  private[analyzer2] val scope: Scope[CT] = Scope(statement.schema, label)

  private[analyzer2] def doRewriteDatabaseNames(state: RewriteDatabaseNamesState) =
    copy(statement = statement.doRewriteDatabaseNames(state))

  def numericate = copy(statement = statement.numericate)

  private[analyzer2] def doRelabel(state: RelabelState) = {
    copy(statement = statement.doRelabel(state),
         label = state.convert(label))
  }

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromStatement[CT, CV] =
    copy(alias = newAlias)

  private[analyzer2] def realTables = Map.empty

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append('(')
    statement.debugStr(sb)
    sb.append(") AS ").append(label)
  }
}

case class FromSingleRow(label: TableLabel, alias: Option[ResourceName]) extends AtomicFrom[Nothing, Nothing] {
  type Self[+CT, +CV] = FromSingleRow

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

  private[analyzer2] def reAlias(newAlias: Option[ResourceName]): FromSingleRow =
    copy(alias = newAlias)

  private[analyzer2] def realTables = Map.empty

  override def debugStr(sb: StringBuilder): StringBuilder = {
    sb.append("@single_row")
  }
}
