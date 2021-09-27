package com.socrata.soql.ast

import scala.util.parsing.input.{NoPosition, Position}
import scala.runtime.ScalaRunTime
import scala.collection.immutable.VectorBuilder

import com.socrata.soql.environment.{ColumnName, FunctionName, HoleName, TableName, TypeName}
import com.socrata.prettyprint.{Doc, LayoutOptions, PageWidth}
import com.socrata.prettyprint.implicits._
import com.socrata.prettyprint.interpolation._

sealed abstract class Expression extends Product {
  val position: Position
  override final def toString = if(Expression.pretty) doc.layoutSmart().toString else ScalaRunTime._toString(this)
  final def toCompactString = doc.layoutPretty(LayoutOptions(pageWidth = PageWidth.Unbounded)).toString

  override final lazy val hashCode = ScalaRunTime._hashCode(this)
  def allColumnRefs: Set[ColumnOrAliasRef]

  def toSyntheticIdentifierBase: String =
    com.socrata.soql.brita.IdentifierFilter(Expression.findIdentsAndLiterals(this))

  def replaceHoles(f: Hole => Expression): Expression

  def doc: Doc[Nothing]
}

object Expression {
  val pretty = AST.pretty

  def escapeString(s: String): String = "'" + s.replaceAll("'", "''") + "'"

  private def findIdentsAndLiterals(e: Expression): Seq[String] = e match {
    case v: Literal => Vector(v.toString)
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
    case Hole(name) =>
      Vector(name.name)
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
  def allColumnRefs = Set(this)
  def replaceHoles(f: Hole => Expression): this.type = this

  def doc = Doc(asString)
}

sealed abstract class Literal extends Expression {
  def allColumnRefs = Set.empty
  def replaceHoles(f: Hole => Expression): this.type = this
}
case class NumberLiteral(value: BigDecimal)(val position: Position) extends Literal {
  def doc = Doc(value.toString)
}
case class StringLiteral(value: String)(val position: Position) extends Literal {
  def doc = Doc("'" + value.replaceAll("'", "''") + "'")
}
case class BooleanLiteral(value: Boolean)(val position: Position) extends Literal {
  def doc = Doc(value.toString.toUpperCase)
}
case class NullLiteral()(val position: Position) extends Literal {
  def doc = d"null"
}

case class FunctionCall(functionName: FunctionName, parameters: Seq[Expression], window: Option[WindowFunctionInfo])(val position: Position, val functionNamePosition: Position) extends Expression  {
  private[ast] def variadizeAssociative(builder: VectorBuilder[Expression]): Unit = {
    require(parameters.length == 2)
    require(window.isEmpty)

    parameters(0) match {
      case fc@FunctionCall(op2, params, None) if op2 == functionName && params.length == 2 =>
        fc.variadizeAssociative(builder)
      case other =>
        builder += other
    }

    parameters(1) match {
      case fc@FunctionCall(op2, params, None) if op2 == functionName && params.length == 2 =>
        fc.variadizeAssociative(builder)
      case other =>
        builder += other
    }
  }

  private def maybeParens(e: Expression) =
    e match {
      case FunctionCall(SpecialFunctions.Parens | SpecialFunctions.Subscript, _, _) => e.doc
      case FunctionCall(SpecialFunctions.Operator(_), _, _) =>
        val edoc = e.doc
        ((d"(" ++ edoc ++ d")") flatAlt edoc).group
      case other => other.doc
    }

  def doc: Doc[Nothing] =
    functionName match {
      case SpecialFunctions.Parens =>
        parameters(0).doc.enclose(d"(", d")")
      case SpecialFunctions.Subscript =>
        parameters(0).doc ++ parameters(1).doc.enclose(d"(", d")")
      case SpecialFunctions.StarFunc(f) =>
        val close = window.fold(Doc.empty) { w => d" " ++ w.doc }
        d"$f(*)" ++ close
      case SpecialFunctions.Operator(op) if parameters.size == 1 =>
        op match {
          case "NOT" =>
            d"NOT ${parameters(0).doc}"
          case _ =>
            d"${op}${parameters(0).doc}"
        }
      case SpecialFunctions.Operator(op) if parameters.size == 2 =>
        op match {
          case "AND" | "OR" | "+" | "*" =>
            // We're going to format this like
            //    blah
            //      OP blah
            //      OP blah
            // taking advantage of the operator's associativity
            val builder = new VectorBuilder[Expression]
            variadizeAssociative(builder)
            val result = builder.result()
            (maybeParens(result.head) +: result.drop(1).map { e => Doc(op) +#+ maybeParens(e) }).sep.hang(2)
          case _ =>
            Seq(maybeParens(parameters(0)), Doc(op) +#+ maybeParens(parameters(1))).sep.hang(2)
        }
      case SpecialFunctions.Operator(op) =>
        sys.error("Found a non-unary, non-binary operator: " + op + " at " + position)
      case SpecialFunctions.Cast(typ) if parameters.size == 1 =>
        d"${parameters(0).doc} :: ${typ.toString}"
      case SpecialFunctions.Between =>
        Seq(parameters(0).doc, d"BETWEEN ${parameters(1).doc}", d"AND ${parameters(2).doc}").sep.hang(2)
      case SpecialFunctions.NotBetween =>
        Seq(parameters(0).doc, d"NOT BETWEEN ${parameters(1).doc}", d"AND ${parameters(2).doc}").sep.hang(2)
      case SpecialFunctions.IsNull =>
        d"${parameters(0).doc} IS NULL"
      case SpecialFunctions.IsNotNull =>
        d"${parameters(0).doc} IS NOT NULL"
      case SpecialFunctions.In =>
        parameters.iterator.drop(1).map(_.doc).toStream.encloseNesting(d"${parameters(0).doc} IN (", Doc.Symbols.comma, d")").group
      case SpecialFunctions.Like =>
        d"${parameters(0).doc} LIKE ${parameters(1).doc}"
      case SpecialFunctions.NotLike =>
        d"${parameters(0).doc} NOT LIKE ${parameters(1).doc}"
      case SpecialFunctions.CountDistinct =>
        val close = window.fold(Doc.empty) { w => d" " ++ w.doc }
        d"COUNT(DISTINCT ${parameters(0).doc})" ++ close
      case SpecialFunctions.Case =>
        val whens = parameters.dropRight(2).grouped(2).map { case Seq(a, b) =>
          Seq(d"WHEN" +#+ a.doc.align, d"THEN" +#+ b.doc.align).sep.nest(2).group
        }.toSeq
        val otherwise = parameters.takeRight(2) match {
          case Seq(BooleanLiteral(true), e) =>
            Some(d"ELSE" +#+ e.doc.align)
          case _ =>
            None
        }
        (whens ++ otherwise).encloseNesting(d"CASE" flatAlt d"CASE ", d"", d"END" flatAlt d" END")
      case other =>
        val close = window.fold(Doc.empty) { w => d" " ++ w.doc }
        if(parameters.lengthCompare(1) == 0) {
          d"${other.toString}(${parameters(0).doc})$close"
        } else {
          parameters.map(_.doc).encloseNesting(
            d"${other.toString}(",
            Doc.Symbols.comma,
            d")" ++ close
          )
        }
    }

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

  lazy val allColumnRefs = parameters.foldLeft(Set.empty[ColumnOrAliasRef])(_ ++ _.allColumnRefs)

  def replaceHoles(f: Hole => Expression): FunctionCall = {
    FunctionCall(functionName, parameters.map(_.replaceHoles(f)), window.map(_.replaceHoles(f)))(position, functionNamePosition)
  }
}

case class WindowFunctionInfo(partitions: Seq[Expression], orderings: Seq[OrderBy], frames: Seq[Expression]) {

  def doc: Doc[Nothing] = {
    val partitionDocs: Option[Doc[Nothing]] =
      if(partitions.nonEmpty) {
        Some((d"PARTITION BY" +: partitions.map(_.doc).punctuate(d",")).sep.hang(2).group)
      } else {
        None
      }
    val orderingDocs: Option[Doc[Nothing]] =
      if(orderings.nonEmpty) {
        Some((d"ORDER BY" +: orderings.map(_.doc).punctuate(d",")).sep.hang(2).group)
      } else {
        None
      }
    val frameDocs: Option[Doc[Nothing]] =
      if(frames.nonEmpty) {
        Some(
          frames.collect {
            case StringLiteral(x) => x
            case NumberLiteral(n) => n.toString
          }.map(Doc(_)).hsep
        )
      } else {
        None
      }

    Seq(partitionDocs, orderingDocs, frameDocs).flatten.encloseNesting(d"OVER (", d"", d")")
  }

  def replaceHoles(f: Hole => Expression): WindowFunctionInfo = {
    WindowFunctionInfo(partitions.map(_.replaceHoles(f)), orderings.map(_.replaceHoles(f)), frames.map(_.replaceHoles(f)))
  }
}

case class Hole(name: HoleName)(val position: Position) extends Expression {
  def allColumnRefs = Set.empty

  def replaceHoles(f: Hole => Expression) = f(this)

  def doc = Doc("?" + name)
}

object bleh extends App {
  val p = new com.socrata.soql.parsing.StandaloneParser
  try {
    println(Select.toDoc(p.binaryTreeSelect(args(0))).layoutPretty(LayoutOptions(PageWidth.AvailablePerLine(args(1).toInt, args(2).toFloat))))
  } catch {
    case e: Exception =>
      println(e.getMessage)
  }
}
