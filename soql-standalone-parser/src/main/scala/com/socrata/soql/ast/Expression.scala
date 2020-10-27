package com.socrata.soql.ast

import scala.util.parsing.input.{NoPosition, Position}
import scala.runtime.ScalaRunTime
import com.socrata.soql.environment.{ColumnName, FunctionName, TableName, TypeName}

sealed abstract class Expression extends Product {
  val position: Position
  protected def asString: String
  override final def toString = if(Expression.pretty) asString else ScalaRunTime._toString(this)
  override final lazy val hashCode = ScalaRunTime._hashCode(this)
  def allColumnRefs: Set[ColumnOrAliasRef]

  def toSyntheticIdentifierBase: String =
    com.socrata.soql.brita.IdentifierFilter(Expression.findIdentsAndLiterals(this))

  // This is used to pretty-print expressions.  It's annoyingly
  // complicated, but fast.  What happens is this: when we're
  // pretty-printing an expression, we want to wrap the arguments if
  // the expression is an overly-long function call, where "overly
  // long" basically means "more than 30 characters from the start of
  // the open parenthesis of the call's argument list".  BUT we don't
  // want to just try rendering it and then checking if it's too long!
  // For a deeply nested expression, that will lead to superlinear
  // (quadratic, I think) runtime.  So what we'll do instead is build
  // our function call into a StringBuilder, and if, _while we're
  // generating that_, we run into our limit, we'll abandon it
  // immediately and return None.
  //
  // Note that if `limit` is `None`, the result will be a `Some`.
  // This is possible to express in Scala, but too clunky to be worth
  // it unfortunately.  The only place that depends on it is
  // FunctionCall's asString implementation.
  def format(depth: Int, sb: StringBuilder, limit: Option[Int]): Option[StringBuilder]
}

object Expression {
  val pretty = AST.pretty

  def escapeString(s: String): String = "'" + s.replaceAll("'", "''") + "'"

  private def findIdentsAndLiterals(e: Expression): Seq[String] = e match {
    case v: Literal => Vector(v.asString)
    case ColumnOrAliasRef(aliasOpt, name) => aliasOpt ++: Vector(name.name)
    case fc: FunctionCall =>
      fc match {
        case FunctionCall(SpecialFunctions.Operator("-"), args, _) => args.flatMap(findIdentsAndLiterals) // otherwise minus looks like a non-synthetic underscore
        case FunctionCall(SpecialFunctions.Operator(op), Seq(arg), _) => op +: findIdentsAndLiterals(arg)
        case FunctionCall(SpecialFunctions.Operator(op), Seq(arg1, arg2), _) => findIdentsAndLiterals(arg1) ++ Vector(op) ++ findIdentsAndLiterals(arg2)
        case FunctionCall(SpecialFunctions.Operator(_), _, _) => sys.error("Found a non-unary, non-binary operator: " + fc)
        case FunctionCall(SpecialFunctions.Cast(typ), Seq(arg), _) => findIdentsAndLiterals(arg) :+ typ.name
        case FunctionCall(SpecialFunctions.Cast(_), _, _) => sys.error("Found a non-unary cast: " + fc)
        case FunctionCall(SpecialFunctions.IsNull, args, _) => args.flatMap(findIdentsAndLiterals) ++ Vector("is", "null")
        case FunctionCall(SpecialFunctions.IsNotNull, args, _) => args.flatMap(findIdentsAndLiterals) ++ Vector("is", "not", "null")
        case FunctionCall(SpecialFunctions.Between, Seq(a,b,c), _) =>
          findIdentsAndLiterals(a) ++ Vector("between") ++ findIdentsAndLiterals(b) ++ Vector("and") ++ findIdentsAndLiterals(c)
        case FunctionCall(SpecialFunctions.NotBetween, Seq(a,b,c), _) =>
          findIdentsAndLiterals(a) ++ Vector("not", "between") ++ findIdentsAndLiterals(b) ++ Vector("and") ++ findIdentsAndLiterals(c)
        case FunctionCall(SpecialFunctions.Case, args, _) =>
          val whens = args.dropRight(2).grouped(2)
          val start = Vector("case") ++ args.dropRight(2).grouped(2).flatMap { case Seq(cond, expr) => Vector("when") ++ findIdentsAndLiterals(cond) ++ Vector("then") ++ findIdentsAndLiterals(expr) }
          val sinon = args.takeRight(2) match {
            case Seq(BooleanLiteral(false), _) => Vector.empty
            case Seq(BooleanLiteral(true), e) => Vector("else") ++ findIdentsAndLiterals(e)
          }
          start ++ sinon ++ Vector("end")
        case FunctionCall(other, args, window) => Vector(other.name) ++ args.flatMap(findIdentsAndLiterals) ++ findIdentsAndLiterals(window)
      }
  }

  private def findIdentsAndLiterals(windowFunctionInfo: Option[WindowFunctionInfo]): Seq[String] =  {
    windowFunctionInfo.toSeq.map { w =>
      val WindowFunctionInfo(partitions, orderings, frames) = w
      val ps = partitions.flatMap(findIdentsAndLiterals)
      val os = orderings.map(_.expression).flatMap(findIdentsAndLiterals)
      Seq("over") ++
        (if (partitions.isEmpty) ps else  "partition_by" +: ps) ++
        (if (orderings.isEmpty) os else  "order_by" +: os) ++
        frames.flatMap(findIdentsAndLiterals)
    }.flatten
  }
}

object SpecialFunctions {
  object StarFunc {
    def apply(f: String) = FunctionName(f + "/*")
    def unapply(f: FunctionName) = f.name match {
      case Regex(x) => Some(x)
      case _ => None
    }
    val Regex = """^(.*)/\*$""".r
  }
  val IsNull = FunctionName("#IS_NULL")
  val Between = FunctionName("#BETWEEN")
  val Like = FunctionName("#LIKE")

  // Case is a little weird -- there's a pre-typecheck verion and a
  // post-typecheck version where it's been resolved to the old "case"
  // function, which is just a normal function.  Bit of a hack, it'd
  // be nice to not need this code to know about the old case
  // function.  Later if we teach soql-pg-adapter about the special
  // function it can just go away...
  val Case = FunctionName("#CASE")
  val CasePostTypecheck = FunctionName("case") // Not actually special in any way!

  // both of these are redundant but needed for synthetic identifiers because we need to
  // distinguish between "not (x is null)" and "x is not null" when generating them.
  val IsNotNull = FunctionName("#IS_NOT_NULL")
  val NotBetween = FunctionName("#NOT_BETWEEN")
  val In = FunctionName("#IN")
  val NotIn = FunctionName("#NOT_IN")
  val NotLike = FunctionName("#NOT_LIKE")

  val CountDistinct = FunctionName("count_distinct")

  // window function: aggregatefunction(x) over (partition by a,b,c...)
  val WindowFunctionOver = FunctionName("#WF_OVER")

  object Operator {
    def apply(op: String) = FunctionName("op$" + op)
    def unapply(f: FunctionName) = f.name match {
      case Regex(x) => Some(x)
      case _ => None
    }
    val Regex = """^op\$(.*)$""".r
  }
  val Subscript = Operator("[]")

  object Field {
    // This is somewhat unfortunate; the first implementation of
    // fields used a simple underscore-separated naming convention
    // rather than defining a SpecialFunction make an untypable thing,
    // and the logs say that the naming convention is in fact actually
    // used in practice.  Therefore unlike all the other
    // SpecialFunctions, this produces names that are actually by
    // users.
    //
    // Also it has no unapply because there is no way to break apart
    // the names back into pieces without knowing what your universe
    // of types are.
    def apply(typ: TypeName, field: String) = FunctionName(typ.name + "_" + field)

    // If we ever do get ourselves out of that (easiest way would be
    // to make the underscorized names (deprecated) aliases for the
    // dot-notation) this object should look something like this,
    // together with a line re-sugaring the function call down below
    // in FunctionCall#asString:

    // def apply(typ: TypeName, field: String) = FunctionName("#FIELD$" + typ.name + "." + field)
    // def unapply(f: FunctionName) = f.name match {
    //   case Regex(t, x) ⇒ Some((TypeName(t), x))
    //   case _ => None
    // }
    // val Regex = """^#FIELD\$(.*)\.(.*)$""".r
  }

  // this exists only so that selecting "(foo)" is never semi-explicitly aliased.
  // it's stripped out by the typechecker.
  val Parens = Operator("()")

  object Cast {
    def apply(op: TypeName) = FunctionName("cast$" + op.name)
    def unapply(f: FunctionName) = f.name match {
      case Regex(x) => Some(TypeName(x))
      case _ => None
    }
    val Regex = """^cast\$(.*)$""".r
  }
}

case class ColumnOrAliasRef(qualifier: Option[String], column: ColumnName)(val position: Position) extends Expression {

  protected def asString = {
    qualifier.map { q =>
      TableName.SoqlPrefix +
        q.substring(TableName.PrefixIndex) +
        TableName.Field
    }.getOrElse("") + "`" + column.name + "`"
  }
  def format(depth: Int, sb: StringBuilder, limit: Option[Int]) = Some(sb.append(asString))
  def allColumnRefs = Set(this)
}

sealed abstract class Literal extends Expression {
  def allColumnRefs = Set.empty
  def format(depth: Int, sb: StringBuilder, limit: Option[Int]) = Some(sb.append(asString))
}
case class NumberLiteral(value: BigDecimal)(val position: Position) extends Literal {
  protected def asString = value.toString
}
case class StringLiteral(value: String)(val position: Position) extends Literal {
  protected def asString = "'" + value.replaceAll("'", "''") + "'"
}
case class BooleanLiteral(value: Boolean)(val position: Position) extends Literal {
  protected def asString = value.toString.toUpperCase
}
case class NullLiteral()(val position: Position) extends Literal {
  override final def asString = "null"
}

case class FunctionCall(functionName: FunctionName, parameters: Seq[Expression], window: Option[WindowFunctionInfo])(val position: Position, val functionNamePosition: Position) extends Expression  {
  protected def asString() = format(0, new StringBuilder, None).get.toString

  private implicit class Interspersable[T](xs: Seq[T]) {
    def intersperse(t: T): Seq[T] = {
      val builder = Seq.newBuilder[T]
      var didOne = false
      for(x <- xs) {
        if(didOne) builder += t
        else didOne = true
        builder += x
      }
      builder.result()
    }
  }

  private def appendParams(sb: StringBuilder, params: Iterator[Expression], sep: String, d: Int, limit: Option[Int]): Option[StringBuilder] = {
    if(params.hasNext) {
      params.next().format(d, sb, limit) match {
        case None => return None
        case Some(_) =>
          limit.foreach { l => if(sb.length > l) return None }
          while(params.hasNext) {
            sb.append(sep)
            params.next().format(d, sb, limit) match {
              case None => return None
              case Some(_) => // ok
            }
            limit.foreach { l => if(sb.length > l) return None }
          }
      }
    }
    Some(sb)
  } // soql-standalone-parser/testOnly com.socrata.soql.parsing.ToStringTest -- -z "wide expressions"

  private def indent(n: Int) = "  " * n

  def format(d: Int, sb: StringBuilder, limit: Option[Int]): Option[StringBuilder] = {
    functionName match {
      case SpecialFunctions.Parens =>
        sb.append("(")
        parameters(0).format(d, sb, limit).map(_.append(")"))
      case SpecialFunctions.Subscript =>
        for {
          sb <- parameters(0).format(d, sb, limit)
          _ = sb.append("[")
          sb <- parameters(1).format(d, sb, limit)
        } yield sb.append("]")
      case SpecialFunctions.StarFunc(f) =>
       sb.append(f).append("(*)")
       window.flatMap(_.format(d, sb, limit)).orElse(Some(sb))
      case SpecialFunctions.Operator(op) if parameters.size == 1 =>
        op match {
          case "NOT" =>
            sb.append("NOT ")
            parameters(0).format(d, sb, limit)
          case _ =>
            sb.append(op)
            parameters(0).format(d, sb, limit)
        }
      case SpecialFunctions.Operator(op) if parameters.size == 2 =>
        for {
          sb <- parameters(0).format(d, sb, limit)
          _ = sb.append(" ").append(op).append(" ")
          sb <- parameters(1).format(d, sb, limit)
        } yield sb
      case SpecialFunctions.Operator(op) =>
        sys.error("Found a non-unary, non-binary operator: " + op + " at " + position)
      case SpecialFunctions.Cast(typ) if parameters.size == 1 =>
        parameters(0).format(d, sb, limit).map(_.append(" :: ").append(typ))
      case SpecialFunctions.Cast(_) =>
        sys.error("Found a non-unary cast at " + position)
      case SpecialFunctions.Between =>
        for {
          sb <- parameters(0).format(d, sb, limit)
          _ = sb.append(" BETWEEN ")
          sb <- parameters(1).format(d, sb, limit)
          _ = sb.append(" AND ")
          sb <- parameters(2).format(d, sb, limit)
        } yield sb
      case SpecialFunctions.NotBetween =>
        for {
          sb <- parameters(0).format(d, sb, limit)
          _ = sb.append(" NOT BETWEEN ")
          sb <- parameters(1).format(d, sb, limit)
          _ = sb.append(" AND ")
          sb <- parameters(2).format(d, sb, limit)
        } yield sb
      case SpecialFunctions.IsNull =>
        parameters(0).format(d, sb, limit).map(_.append(" IS NULL"))
      case SpecialFunctions.IsNotNull =>
        parameters(0).format(d, sb, limit).map(_.append(" IS NOT NULL"))
      case SpecialFunctions.In =>
        parameters(0).format(d, sb, limit).map(_.append(" IN (")).flatMap { sb =>
          appendParams(sb, parameters.iterator.drop(1), ",", d, limit)
        }.map(_.append(")"))
      case SpecialFunctions.NotIn =>
        parameters(0).format(d, sb, limit).map(_.append(" NOT IN (")).flatMap { sb =>
          appendParams(sb, parameters.iterator.drop(1), ",", d, limit)
        }.map(_.append(")"))
      case SpecialFunctions.Like =>
        appendParams(sb, parameters.iterator, " LIKE ", d, limit)
      case SpecialFunctions.NotLike =>
        appendParams(sb, parameters.iterator, " NOT LIKE ", d, limit)
      case SpecialFunctions.CountDistinct =>
        sb.append("COUNT(DISTINCT ")
        for {
          sb <- parameters(0).format(d, sb, limit)
          _ = sb.append(")")
        } yield sb
      case SpecialFunctions.Case =>
        sb.append("CASE")
        for(Seq(a, b) <- parameters.dropRight(2).grouped(2)) {
          sb.append('\n').append(indent(d+2)).append("WHEN ")
          a.format(d+1, sb, None)
          sb.append('\n').append(indent(d+4)).append("THEN ")
          b.format(d+2, sb, None)
        }
        parameters.takeRight(2) match {
          case Seq(BooleanLiteral(true), e) =>
            sb.append('\n').append(indent(d+2)).append("ELSE ")
            e.format(d+1, sb, None)
          case Seq(BooleanLiteral(false), _) =>
            // no else
        }
        sb.append('\n').append(indent(d)).append("END")
        Some(sb)
      case other => {
        formatBase(sb, d, limit, other, parameters)
        window.flatMap(_.format(d, sb, limit)).orElse(Some(sb))
      }
    }
  }

  private def formatBase(sb: StringBuilder, d: Int, limit: Option[Int], other: FunctionName, parameters: Seq[Expression]): Option[StringBuilder] = {
    sb.append(other).append("(")
    val startLength = sb.length
    appendParams(sb, parameters.iterator, ", ", d, limit.orElse(Some(startLength + 30))) match {
      case Some(sb) =>
        // it all fit on one line
        sb.append(")")
        Some(sb)
      case None =>
        limit match {
          case Some(l) =>
            None
          case None =>
            // it didn't, we'll need to line-break
            sb.setLength(startLength) // undo anything that got written
            sb.append("\n").append(indent(d+1))
            appendParams(sb, parameters.iterator, ",\n" + indent(d+1), d+1, None)
            sb.append("\n").append(indent(d))
            Some(sb.append(")"))
        }
    }
  }

  lazy val allColumnRefs = parameters.foldLeft(Set.empty[ColumnOrAliasRef])(_ ++ _.allColumnRefs)
}

case class WindowFunctionInfo(partitions: Seq[Expression], orderings: Seq[OrderBy], frames: Seq[Expression]) {

  def format(d: Int, sb: StringBuilder, limit: Option[Int]): Option[StringBuilder] = {
    sb.append(" OVER (")
    if (partitions.nonEmpty) sb.append(" PARTITION BY ")
    sb.append(partitions.map(_.toString).mkString(", "))
    if (orderings.nonEmpty) sb.append(" ORDER BY ")
    sb.append(orderings.map(_.toString).mkString(", "))
    frames.foreach {
      case StringLiteral(x) => sb.append(" "); sb.append(x)
      case NumberLiteral(n) => sb.append(" "); sb.append(n)
      case _ =>
    }
    sb.append(")")
    Some(sb)
  }
}
