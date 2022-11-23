package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.typechecker.HasDoc

trait JoinImpl[+RNS, +CT, +CV] { this: Join[RNS, CT, CV] =>
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
          loop(j.right.addToEnvironment[CT2] _ :: stack, j.left)
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

  private[analyzer2] def columnReferences: Map[TableLabel, Set[ColumnLabel]] =
    reduce[Map[TableLabel, Set[ColumnLabel]]](
      _.columnReferences,
      (acc, join) => acc.mergeWith(join.right.columnReferences)(_ ++ _).mergeWith(join.on.columnReferences)(_ ++ _)
    )

  private[analyzer2] def doRemoveUnusedColumns(used: Map[TableLabel, Set[ColumnLabel]]): Self[RNS, CT, CV] =
    map[RNS, CT, CV](
      _.doRemoveUnusedColumns(used),
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, right.doRemoveUnusedColumns(used), on) }
    )

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

  private[analyzer2] def doLabelMap[RNS2 >: RNS](state: LabelMapState[RNS2]): Unit = {
    reduce[Unit](
      _.doLabelMap(state),
      (_, join) => join.right.doLabelMap(state)
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

  def find(predicate: Expr[CT, CV] => Boolean): Option[Expr[CT, CV]] =
    reduce[Option[Expr[CT, CV]]](
      _.find(predicate),
      { (acc, join) => acc.orElse(join.right.find(predicate)).orElse(join.on.find(predicate)) }
    )
  def contains[CT2 >: CT, CV2 >: CV](e: Expr[CT2, CV2]): Boolean =
    reduce[Boolean](_.contains(e), { (acc, join) => acc || join.right.contains(e) || join.on.contains(e) })

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

trait OJoinImpl { this: Join.type =>
  implicit def serialize[RNS: Writable, CT: Writable, CV](implicit ev: Writable[Expr[CT, CV]]): Writable[Join[RNS, CT, CV]] = new Writable[Join[RNS, CT, CV]] {
    val afSer = AtomicFrom.serialize[RNS, CT, CV]

    def writeTo(buffer: WriteBuffer, join: Join[RNS, CT, CV]): Unit = {
      buffer.write(join.reduce[Int](_ => 0, (n, _) => n + 1))
      join.reduce[Unit](
        f0 => afSer.writeTo(buffer, f0),
        { (_, rhs) =>
          val Join(joinType, lateral, _left, right, on) = rhs
          buffer.write(joinType)
          buffer.write(lateral)
          afSer.writeTo(buffer, right)
          buffer.write(on)
        }
      )
    }
  }

  implicit def deserialize[RNS: Readable, CT: Readable, CV](implicit ev: Readable[Expr[CT, CV]]): Readable[Join[RNS, CT, CV]] =
    new Readable[Join[RNS, CT, CV]] {
      val afDer = AtomicFrom.deserialize[RNS, CT, CV]

      def readFrom(buffer: ReadBuffer): Join[RNS, CT, CV] = {
        val joins = buffer.read[Int]()
        if(joins < 1) fail("Invalid join count " + joins)

        @tailrec
        def loop(left: From[RNS, CT, CV], n: Int): Join[RNS, CT, CV] = {
          val j = Join(
            joinType = buffer.read[JoinType](),
            lateral = buffer.read[Boolean](),
            left = left,
            right = afDer.readFrom(buffer),
            on = buffer.read[Expr[CT, CV]]()
          )

          if(n > 1) {
            loop(j, n-1)
          } else {
            j
          }
        }
        loop(afDer.readFrom(buffer), joins)
      }
    }
}
