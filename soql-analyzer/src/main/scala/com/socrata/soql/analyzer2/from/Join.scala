package com.socrata.soql.analyzer2.from

import scala.annotation.tailrec
import scala.collection.compat.immutable.LazyList

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.serialization.{Readable, ReadBuffer, Writable, WriteBuffer}
import com.socrata.soql.collection._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.functions.MonomorphicFunction

trait JoinImpl[MT <: MetaTypes] { this: Join[MT] =>
  type Self[MT <: MetaTypes] = Join[MT]
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
  private[analyzer2] def extendEnvironment(base: Environment[MT]): Either[AddScopeError, Environment[MT]] = {
    type Stack = List[Environment[MT] => Either[AddScopeError, Environment[MT]]]

    @tailrec
    def loop(stack: Stack, self: From[MT]): (Stack, AtomicFrom[MT]) = {
      self match {
        case j: Join[MT] =>
          loop(j.right.addToEnvironment _ :: stack, j.left)
        case other: AtomicFrom[MT] =>
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

  type ReduceResult[MT <: MetaTypes] = Join[MT]

  override def reduceMap[S, MT2 <: MetaTypes](
    base: AtomicFrom[MT] => (S, AtomicFrom[MT2]),
    combine: (S, JoinType, Boolean, From[MT2], AtomicFrom[MT], Expr[MT]) => (S, Join[MT2])
  ): (S, ReduceResult[MT2]) = {
    type Stack = List[(S, From[MT2]) => (S, Join[MT2])]

    @tailrec
    def loop(self: From[MT], stack: Stack): (S, From[MT2]) = {
      self match {
        case Join(joinType, lateral, left, right, on) =>
          loop(left, { (s, newLeft) => combine(s, joinType, lateral, newLeft, right, on) } :: stack)
        case leftmost: AtomicFrom[MT] =>
          stack.foldLeft[(S, From[MT2])](base(leftmost)) { (acc, f) =>
            val (s, left) = acc
            f(s, left)
          }
      }
    }

    loop(this, Nil).asInstanceOf[(S, Join[MT2])]
  }

  private[analyzer2] def columnReferences: Map[AutoTableLabel, Set[ColumnLabel]] =
    reduce[Map[AutoTableLabel, Set[ColumnLabel]]](
      _.columnReferences,
      (acc, join) => acc.mergeWith(join.right.columnReferences)(_ ++ _).mergeWith(join.on.columnReferences)(_ ++ _)
    )

  private[analyzer2] def realTables: Map[AutoTableLabel, DatabaseTableName] = {
    reduce[Map[AutoTableLabel, DatabaseTableName]] (
      { other => other.realTables },
      { (acc, j) => acc ++ j.right.realTables }
    )
  }

  private[analyzer2] def doRewriteDatabaseNames[MT2 <: MetaTypes](state: RewriteDatabaseNamesState[MT2]): Join[MT2] = {
    map[MT2](
       _.doRewriteDatabaseNames(state),
       { (joinType, lateral, left, right, on) =>
         val newRight = right.doRewriteDatabaseNames(state)
         val newOn = on.doRewriteDatabaseNames(state)
         Join(joinType, lateral, left, newRight, newOn)
       }
    )
  }

  private[analyzer2] def doRelabel(state: RelabelState): Join[MT] = {
    map[MT](
      _.doRelabel(state),
      { (joinType, lateral, left, right, on) =>
        val newRight = right.doRelabel(state)
        val newOn = on.doRelabel(state)
        Join(joinType, lateral, left, newRight, newOn)
      }
    )
  }

  private[analyzer2] def doLabelMap(state: LabelMapState[MT]): Unit = {
    reduce[Unit](
      _.doLabelMap(state),
      (_, join) => join.right.doLabelMap(state)
    )
  }

  def mapAlias(f: Option[ResourceName] => Option[ResourceName]): Self[MT] =
    map[MT](
      _.mapAlias(f),
      { (joinType, lateral, left, right, on) => Join(joinType, lateral, left, right.mapAlias(f), on) },
    )

  def find(predicate: Expr[MT] => Boolean): Option[Expr[MT]] =
    reduce[Option[Expr[MT]]](
      _.find(predicate),
      { (acc, join) => acc.orElse(join.right.find(predicate)).orElse(join.on.find(predicate)) }
    )
  def contains(e: Expr[MT]): Boolean =
    reduce[Boolean](_.contains(e), { (acc, join) => acc || join.right.contains(e) || join.on.contains(e) })

  private[analyzer2] final def findIsomorphism(state: IsomorphismState, that: From[MT]): Boolean =
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

  private[analyzer2] def doDebugDoc(implicit ev: StatementDocProvider[MT]) =
    reduce[Doc[Annotation[MT]]](
      _.doDebugDoc,
      { (leftDoc, j) =>
        val Join(joinType, lateral, _left, right, on) = j
        Seq(
          leftDoc,
          Seq(
            Some(joinType.debugDoc),
            if(lateral) Some(d"LATERAL") else None,
            Some(right.doDebugDoc),
            Some(d"ON"),
            Some(on.debugDoc(ev))
          ).flatten.hsep
        ).sep
      },
    )

  def unique =
    reduce[LazyList[List[Seq[Column[MT]]]]](
      _.unique.to(LazyList).map(_ :: Nil),
      { (uniqueSoFar, join) =>
        uniqueSoFar.flatMap { uniques =>
          join.right.unique.map(_ :: uniques)
        }
      }
    ).map(_.reverse.flatten)
}

trait OJoinImpl { this: Join.type =>
  implicit def serialize[MT <: MetaTypes](implicit rnsWritable: Writable[MT#ResourceNameScope], ctWritable: Writable[MT#ColumnType], exprWritable: Writable[Expr[MT]], dtnWritable: Writable[MT#DatabaseTableNameImpl], dcnWritable: Writable[MT#DatabaseColumnNameImpl]): Writable[Join[MT]] = new Writable[Join[MT]] {
    val afSer = AtomicFrom.serialize[MT]

    def writeTo(buffer: WriteBuffer, join: Join[MT]): Unit = {
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

  implicit def deserialize[MT <: MetaTypes](implicit rnsReadable: Readable[MT#ResourceNameScope], ctReadable: Readable[MT#ColumnType], exprReadable: Readable[Expr[MT]], dtnReadable: Readable[MT#DatabaseTableNameImpl], dcnReadable: Readable[MT#DatabaseColumnNameImpl]): Readable[Join[MT]] =
    new Readable[Join[MT]] with MetaTypeHelper[MT] {
      val afDer = AtomicFrom.deserialize[MT]

      def readFrom(buffer: ReadBuffer): Join[MT] = {
        val joins = buffer.read[Int]()
        if(joins < 1) fail("Invalid join count " + joins)

        @tailrec
        def loop(left: From[MT], n: Int): Join[MT] = {
          val j = Join(
            joinType = buffer.read[JoinType](),
            lateral = buffer.read[Boolean](),
            left = left,
            right = afDer.readFrom(buffer),
            on = buffer.read[Expr[MT]]()
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
