package com.socrata.soql.environment

import scala.util.parsing.input.{Position, NoPosition}

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{SimpleHierarchyEncodeBuilder, SimpleHierarchyDecodeBuilder, InternalTag, AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder}

import com.socrata.soql.jsonutils.HierarchyImplicits._
import com.socrata.soql.jsonutils.PositionCodec

sealed abstract class Source[+RNS] {
  val scopedResourceName: Option[ScopedResourceName[RNS]]
  val position: Position
  def withPosition(pos: Position): Source[RNS]
  def mapSRN[RNS2](f: ScopedResourceName[RNS] => ScopedResourceName[RNS2]): Source[RNS2]
}

object Source {
  /** This source refers to a position within un-saved soql */
  case class Anonymous(position: Position) extends Source[Nothing] {
    override val scopedResourceName = None
    override def withPosition(pos: Position) = Anonymous(pos)
    override def mapSRN[RNS2](f: ScopedResourceName[Nothing] => ScopedResourceName[RNS2]) = this
  }
  /** This source was generated and has no corresponding soql text */
  case object Synthetic extends Source[Nothing] {
    override val scopedResourceName = None
    override val position = NoPosition
    override def withPosition(pos: Position) = Synthetic
    override def mapSRN[RNS2](f: ScopedResourceName[Nothing] => ScopedResourceName[RNS2]) = this
  }
  /** This source refers to a position within saved soql (which therefore has an associated resource name) */
  case class Saved[+RNS](definiteScopedResourceName: ScopedResourceName[RNS], position: Position) extends Source[RNS] {
    val scopedResourceName = Some(definiteScopedResourceName)
    override def withPosition(pos: Position) = copy(position = pos)
    override def mapSRN[RNS2](f: ScopedResourceName[RNS] => ScopedResourceName[RNS2]) =
      copy(definiteScopedResourceName = f(definiteScopedResourceName))
  }

  def nonSynthetic[RNS](scopedResourceName: Option[ScopedResourceName[RNS]], pos: Position): Source[RNS] =
    scopedResourceName match {
      case None => Anonymous(pos)
      case Some(s) => Saved(s, pos)
    }

  private val Tag = "type"
  private val SyntheticSource = "synthetic"
  private val AnonSource = "anonymous"
  private val SavedSource = "saved"
  implicit def encode[RNS: JsonEncode] = SimpleHierarchyEncodeBuilder[Source[RNS]](InternalTag(Tag))
    .singleton(SyntheticSource, Synthetic)
    .branch[Anonymous](AnonSource)(AutomaticJsonEncodeBuilder[Anonymous], implicitly)
    .branch[Saved[RNS]](SavedSource)(AutomaticJsonEncodeBuilder[Saved[RNS]], implicitly)
    .build

  implicit def decode[RNS: JsonDecode] = SimpleHierarchyDecodeBuilder[Source[RNS]](InternalTag(Tag))
    .singleton(SyntheticSource, Synthetic)
    .branch[Anonymous](AnonSource)(AutomaticJsonDecodeBuilder[Anonymous], implicitly)
    .branch[Saved[RNS]](SavedSource)(AutomaticJsonDecodeBuilder[Saved[RNS]], implicitly)
    .build
}
