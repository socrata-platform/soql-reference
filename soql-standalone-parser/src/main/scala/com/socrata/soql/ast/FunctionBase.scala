package com.socrata.soql.ast

import com.socrata.soql.environment.FunctionName

import scala.util.parsing.input.Position

trait FunctionBase {
  val functionName: FunctionName
  val parameters: Seq[Expression]

  val position: Position
  val functionNamePosition: Position

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

  protected def appendParams(sb: StringBuilder, params: Iterator[Expression], sep: String, d: Int, limit: Option[Int]): Option[StringBuilder] = {
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

  protected def indent(n: Int) = "  " * n

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
        Some(sb.append(f).append("(*)"))
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
      case other => {
        formatBase(sb, d, limit, other, parameters)
//        sb.append(other).append("(")
//        val startLength = sb.length
//        appendParams(sb, parameters.iterator, ", ", d, limit.orElse(Some(startLength + 30))) match {
//          case Some(sb) =>
//            // it all fit on one line
//            sb.append(")")
//            Some(sb)
//          case None =>
//            limit match {
//              case Some(l) =>
//                None
//              case None =>
//                // it didn't, we'll need to line-break
//                sb.setLength(startLength) // undo anything that got written
//                sb.append("\n").append(indent(d+1))
//                appendParams(sb, parameters.iterator, ",\n" + indent(d+1), d+1, None)
//                sb.append("\n").append(indent(d))
//                Some(sb.append(")"))
//            }
//        }
      }
    }
  }

  protected  def formatBase(sb: StringBuilder, d: Int, limit: Option[Int], other: FunctionName, parameters: Seq[Expression]): Option[StringBuilder] = {
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

  private def windowOverPartition(es: Seq[Expression]): Seq[Expression] = {
    val ps = es.dropWhile {
      case StringLiteral("partition_by") => false
      case _ => true
    }
    val ps1 = ps.takeWhile {
      case StringLiteral("order_by") => false
      case StringLiteral("range") => false
      case StringLiteral("rows") => false
      case _ => true
    }
    if (ps1.nonEmpty) ps1.tail
    else ps1
  }

  private def windowOverOrder(es: Seq[Expression]): Seq[Expression] = {
    val ps = es.dropWhile {
      case StringLiteral("order_by") => false
      case _ => true
    }
    val ps1 = ps.takeWhile {
      case StringLiteral("range") => false
      case StringLiteral("rows") => false
      case _ => true
    }
    if (ps1.nonEmpty) ps1.tail
    else ps1
  }

  private def windowOverFrame(es: Seq[Expression]): Seq[Expression] = {
    val ps = es.dropWhile {
      case StringLiteral("range") => false
      case StringLiteral("rows") => false
      case _ => true
    }
    ps
  }

  lazy val allColumnRefs = parameters.foldLeft(Set.empty[ColumnOrAliasRef])(_ ++ _.allColumnRefs)
}
