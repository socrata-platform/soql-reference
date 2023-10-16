package com.socrata.soql.util

import com.rojoma.json.v3.ast.{JValue, JObject, JString}
import com.rojoma.json.v3.codec.{JsonDecode, DecodeError, Path}

// Like a SimpleHierarchyDecodeBuilder but even simpler: it only does
// non-removed internal tags, and it doesn't try to prevent you from
// adding the same leaf type more than once (just prevents you from
// reusing the same leaf tag name).
private[util] class SimplerHierarchyDecodeBuilder[Root] private (typeField: String, subcodecs: Map[String, JsonDecode[_ <: Root]]) {
  def branch[T <: Root](name: String)(implicit dec: JsonDecode[T]) = {
    if(subcodecs contains name) throw new IllegalArgumentException("Already defined a decoder for branch " + name)
    new SimplerHierarchyDecodeBuilder[Root](typeField, subcodecs + (name -> dec))
  }

  def build: JsonDecode[Root] = {
    if(subcodecs.isEmpty) throw new IllegalStateException("No branches defined")
    new JsonDecode[Root] {
      def decode(x: JValue): Either[DecodeError, Root] = x match {
        case JObject(fields) =>
          fields.get(typeField) match {
            case Some(jTypeTag@JString(typeTag)) =>
              subcodecs.get(typeTag) match {
                case Some(subDec) =>
                  subDec.decode(x) // no need to augment error results since we're not moving downward
                case None =>
                  Left(DecodeError.InvalidValue(jTypeTag, Path(typeField)))
              }
            case Some(other) =>
              Left(DecodeError.InvalidType(expected = JString, got = other.jsonType, Path(typeField)))
            case None =>
              Left(DecodeError.MissingField(typeField))
          }
        case other =>
          Left(DecodeError.InvalidType(expected = JObject, got = other.jsonType))
      }
    }
  }
}

private[util] object SimplerHierarchyDecodeBuilder {
  def apply[Root](tagField: String) = new SimplerHierarchyDecodeBuilder[Root](tagField, Map.empty)
}
