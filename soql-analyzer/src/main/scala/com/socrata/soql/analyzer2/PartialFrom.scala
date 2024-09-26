package com.socrata.soql.analyzer2

import scala.annotation.tailrec

import com.socrata.soql.collection._
import com.socrata.soql.ast

private[analyzer2] sealed abstract class PartialFrom[MT <: MetaTypes] extends StatementUniverse[MT] {
  def extendEnvironment(base: Environment[MT]): Either[AddScopeError, Environment[MT]]

  def listOfAtomics = {
    @tailrec
    def loop(self: PartialFrom[MT], suffix: List[AtomicFrom]): NonEmptySeq[AtomicFrom] =
      self match {
        case j: PartialFrom.PartialJoin[MT] => loop(j.left, j.right::suffix)
        case PartialFrom.Atomic(af) => NonEmptySeq(af, suffix)
      }
    loop(this, Nil)
  }
}

private[analyzer2] object PartialFrom {
  final case class PartialJoin[MT <: MetaTypes](joinType: JoinType, lateral: Boolean, left: PartialFrom[MT], right: AtomicFrom[MT], on: ast.Expression) extends PartialFrom[MT] {
    def extendEnvironment(base: Environment[MT]): Either[AddScopeError, Environment[MT]] = {
      type Stack = List[Environment[MT] => Either[AddScopeError, Environment[MT]]]

      @tailrec
      def loop(stack: Stack, self: PartialFrom[MT]): (Stack, AtomicFrom) = {
        self match {
          case j: PartialJoin[MT] =>
            loop(j.right.addToEnvironment _ :: stack, j.left)
          case Atomic(other) =>
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
  }

  case class Atomic[MT <: MetaTypes](from: AtomicFrom[MT]) extends PartialFrom[MT] {
    override def extendEnvironment(base: Environment[MT]) = from.extendEnvironment(base)
  }
}
