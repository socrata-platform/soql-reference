package com.socrata.soql.analyzer2.rewrite

import com.rojoma.json.v3.ast.{JValue, JObject}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{SimpleHierarchyCodecBuilder, InternalTag, AutomaticJsonDecodeBuilder, AutomaticJsonEncodeBuilder}

import com.socrata.soql.serialize.{ReadBuffer, WriteBuffer, Readable, Writable}
import com.socrata.soql.jsonutils.HierarchyImplicits._

// These classes' json and binary serializations are written such that
// the subclass relationship holds in their serialized
// representations - that is, if you serialize a Pass or a
// DangerousPass on one end, you can deserialize an AnyPass on the
// other.  To do this, Pass and DangerousPass need to agree on their
// tagging schema - for JSON this is easy (just don't reuse any name,
// which would be confusing anyway); for binary dangerous passes' tags
// are negative and ordinary passes' tags are non-negative.
//
// A "dangerous" pass is one that can leak information.  The canonical
// (and, at the time of this writing, only) pass like this is
// PreserveOrderingWithColumns, which, when possible, rewrites the
// query in such a way that any expressions used in the ORDER BY
// clause get added to the select-list if they're not already there.
sealed abstract class AnyPass
sealed abstract class Pass extends AnyPass
sealed abstract class DangerousPass extends AnyPass

object AnyPass {
  private[rewrite] def codecBase[T <: AnyPass] = SimpleHierarchyCodecBuilder[T](InternalTag("pass"))

  implicit val jCodec =
    DangerousPass.passBuilder(Pass.passBuilder(codecBase[AnyPass])).build

  implicit object serialize extends Readable[AnyPass] with Writable[AnyPass] {
    def readFrom(buffer: ReadBuffer): AnyPass = {
      val tag = buffer.read[Int]()
      if(tag < 0) DangerousPass.serialize.readTaggedFrom(buffer, tag)
      else Pass.serialize.readTaggedFrom(buffer, tag)
    }

    def writeTo(buffer: WriteBuffer, rp: AnyPass): Unit = {
      rp match {
        case dp: DangerousPass => DangerousPass.serialize.writeTo(buffer, dp)
        case p: Pass => Pass.serialize.writeTo(buffer, p)
      }
    }
  }
}

object Pass {
  case object InlineTrivialParameters extends Pass
  case object PreserveOrdering extends Pass
  case object RemoveTrivialSelects extends Pass
  case object ImposeOrdering extends Pass
  case object Merge extends Pass
  case object RemoveUnusedColumns extends Pass
  case object RemoveUnusedOrderBy extends Pass
  case object UseSelectListReferences extends Pass
  case class Page(size: NonNegativeBigInt, offset: NonNegativeBigInt) extends Pass
  case class AddLimitOffset(limit: Option[NonNegativeBigInt], offset: Option[NonNegativeBigInt]) extends Pass
  case object RemoveOrderBy extends Pass
  case class LimitIfUnlimited(limit: NonNegativeBigInt) extends Pass
  case object RemoveTrivialJoins extends Pass
  case object RemoveSyntheticColumns extends Pass
  case object RemoveSystemColumns extends Pass

  private[rewrite] def passBuilder[T >: Pass <: AnyRef](builder: SimpleHierarchyCodecBuilder[T]): SimpleHierarchyCodecBuilder[T] =
    builder
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
      .singleton("remove_order_by", RemoveOrderBy)
      .branch[LimitIfUnlimited]("limit_if_unlimited")(AutomaticJsonEncodeBuilder[LimitIfUnlimited], AutomaticJsonDecodeBuilder[LimitIfUnlimited], implicitly)
      .singleton("remove_trivial_joins", RemoveTrivialJoins)
      .singleton("remove_synthetic_columns", RemoveSyntheticColumns)
      .singleton("remove_system_columns", RemoveSystemColumns)

  implicit val jCodec = passBuilder(AnyPass.codecBase[Pass]).build

  implicit object serialize extends Readable[Pass] with Writable[Pass] {
    private[rewrite] def readTaggedFrom(buffer: ReadBuffer, tag: Int): Pass =
      tag match {
        case 0 => InlineTrivialParameters
        case 1 => PreserveOrdering
        case 2 => RemoveTrivialSelects
        case 3 => ImposeOrdering
        case 4 => Merge
        case 5 => RemoveUnusedColumns
        case 6 => RemoveUnusedOrderBy
        case 7 => UseSelectListReferences
        case 8 => Page(buffer.read[NonNegativeBigInt](), buffer.read[NonNegativeBigInt]())
        case 9 => AddLimitOffset(buffer.read[Option[NonNegativeBigInt]](), buffer.read[Option[NonNegativeBigInt]]())
        case 10 => RemoveOrderBy
        case 11 => LimitIfUnlimited(buffer.read[NonNegativeBigInt]())
        case 12 => RemoveTrivialJoins
        case 13 => RemoveSyntheticColumns
        case 14 => RemoveSystemColumns
        case other => fail(s"Unknown rewrite pass type $other")
      }

    def readFrom(buffer: ReadBuffer): Pass =
      readTaggedFrom(buffer, buffer.read[Int]())

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
        case RemoveOrderBy => buffer.write(10)
        case LimitIfUnlimited(lim) =>
          buffer.write(11)
          buffer.write(lim)
        case RemoveTrivialJoins =>
          buffer.write(12)
        case RemoveSyntheticColumns =>
          buffer.write(13)
        case RemoveSystemColumns =>
          buffer.write(14)
      }
    }
  }
}

object DangerousPass {
  case object PreserveOrderingWithColumns extends DangerousPass

  private[rewrite] def passBuilder[T >: DangerousPass <: AnyRef](builder: SimpleHierarchyCodecBuilder[T]): SimpleHierarchyCodecBuilder[T] =
    builder
      .singleton("preserve_ordering_with_columns", PreserveOrderingWithColumns)

  implicit val jCodec = passBuilder(AnyPass.codecBase[DangerousPass]).build

  implicit object serialize extends Readable[DangerousPass] with Writable[DangerousPass] {
    private[rewrite] def readTaggedFrom(buffer: ReadBuffer, tag: Int): DangerousPass =
      tag match {
        // tag -1 is deliberately skipped; while I don't think we'll
        // ever add a third (or fourth etc etc) category, keeping -1
        // free leaves a compactly-represented hole which we can use
        // for signalling those further categories later.
        case -2 => PreserveOrderingWithColumns
        case other => fail(s"Unknown dangerous rewrite pass type $other")
      }

    def readFrom(buffer: ReadBuffer): DangerousPass =
      readTaggedFrom(buffer, buffer.read[Int]())

    def writeTo(buffer: WriteBuffer, rp: DangerousPass): Unit = {
      rp match {
        case PreserveOrderingWithColumns => buffer.write(-2)
      }
    }
  }
}
