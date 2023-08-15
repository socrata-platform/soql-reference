package com.socrata.soql.analyzer2.rewrite

import scala.reflect.ClassTag

import com.rojoma.json.v3.ast.{JValue, JObject}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{SimpleHierarchyCodecBuilder, InternalTag, AutomaticJsonDecodeBuilder, AutomaticJsonEncodeBuilder}

import com.socrata.soql.serialize.{ReadBuffer, WriteBuffer, Readable, Writable}

sealed abstract class Pass

object Pass {
  case object InlineTrivialParameters extends Pass
  case object PreserveOrdering extends Pass
  case object RemoveTrivialSelects extends Pass
  case object ImposeOrdering extends Pass
  case object Merge extends Pass
  case object RemoveUnusedColumns extends Pass
  case object RemoveUnusedOrderBy extends Pass
  case object UseSelectListReferences extends Pass
  case class Page(size: BigInt, offset: BigInt) extends Pass
  case class AddLimitOffset(limit: Option[BigInt], offset: Option[BigInt]) extends Pass
  case object RemoveAllOrderBy extends Pass

  private class SingletonCodec[T](value: T) extends JsonEncode[T] with JsonDecode[T] {
    def encode(t: T) = JObject.canonicalEmpty
    def decode(j: JValue) =
      j match {
        case obj: JObject => Right(value)
        case other => Left(DecodeError.InvalidType(expected = JObject, got = other.jsonType))
      }
  }
  implicit class AugmentedSHCB[T <: AnyRef](private val underlying: SimpleHierarchyCodecBuilder[T]) extends AnyVal {
    def singleton[U <: T : ClassTag](tag: String, value: U) = {
      implicit val c = new SingletonCodec(value)
      underlying.branch[U](tag)
    }
  }

  implicit val jCodec = SimpleHierarchyCodecBuilder[Pass](InternalTag("pass"))
    .singleton("inline_trivial_parameters", InlineTrivialParameters)
    .singleton("preserve_ordering", PreserveOrdering)
    .singleton("remove_trivial_selects", RemoveTrivialSelects)
    .singleton("impose_ordering", ImposeOrdering)
    .singleton("merge", Merge)
    .singleton("remove_unused_columns", RemoveUnusedColumns)
    .singleton("remove_unused_order_by", RemoveUnusedOrderBy)
    .singleton("use_select_list_references", UseSelectListReferences)
    .branch[Page]("page")(AutomaticJsonEncodeBuilder[Page], AutomaticJsonDecodeBuilder[Page], implicitly)
    .branch[AddLimitOffset]("add_limit_offset")(AutomaticJsonEncodeBuilder[AddLimitOffset], AutomaticJsonDecodeBuilder[AddLimitOffset], implicitly)
    .singleton("remove_all_order_by", RemoveAllOrderBy)
    .build

  implicit object serialize extends Readable[Pass] with Writable[Pass] {
    def readFrom(buffer: ReadBuffer): Pass =
      buffer.read[Int]() match {
        case 0 => InlineTrivialParameters
        case 1 => PreserveOrdering
        case 2 => RemoveTrivialSelects
        case 3 => ImposeOrdering
        case 4 => Merge
        case 5 => RemoveUnusedColumns
        case 6 => RemoveUnusedOrderBy
        case 7 => UseSelectListReferences
        case 8 => Page(buffer.read[BigInt](), buffer.read[BigInt]())
        case 9 => AddLimitOffset(buffer.read[Option[BigInt]](), buffer.read[Option[BigInt]]())
        case 10 => RemoveAllOrderBy
        case other => fail(s"Unknown rewrite pass type $other")
      }

    def writeTo(buffer: WriteBuffer, rp: Pass): Unit = {
      rp match {
        case InlineTrivialParameters => buffer.write(0)
        case PreserveOrdering => buffer.write(1)
        case RemoveTrivialSelects => buffer.write(2)
        case ImposeOrdering => buffer.write(3)
        case Merge => buffer.write(4)
        case RemoveUnusedColumns => buffer.write(5)
        case RemoveUnusedOrderBy => buffer.write(6)
        case UseSelectListReferences => buffer.write(7)
        case Page(size, off) =>
          buffer.write(8)
          buffer.write(size)
          buffer.write(off)
        case AddLimitOffset(lim, off) =>
          buffer.write(9)
          buffer.write(lim)
          buffer.write(off)
        case RemoveAllOrderBy => buffer.write(10)
      }
    }
  }
}

