package com.socrata.soql.util

import scala.reflect.ClassTag

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec._

private[util] class ErrorHierarchyDecodeBuilder[Root <: AnyRef] private (subcodecs: Map[String, JsonDecode[_ <: Root]], classes: Map[Class[_], String]) {
  def this() = this(Map.empty, Map.empty)

  def branch[T <: Root](implicit dec: SoQLErrorDecode[T], mfst: ClassTag[T]) = {
    val name = dec.code
    val cls = mfst.runtimeClass
    if(subcodecs.contains(name)) throw new IllegalArgumentException("Already defined a decoder for branch " + name)
    if(classes.contains(cls)) throw new IllegalArgumentException("Already defined a decoder for class " + cls)
    new ErrorHierarchyDecodeBuilder[Root](subcodecs + (name -> SoQLErrorCodec.jsonDecode(dec)), classes + (cls -> name))
  }

  def build: JsonDecode[Root] = {
    if(subcodecs.isEmpty) throw new IllegalStateException("No branches defined")
    new JsonDecode[Root] {
      def decode(x: JValue): Either[DecodeError, Root] = x match {
        case JObject(fields) =>
          fields.get("type") match {
            case Some(jTypeTag@JString(typeTag)) =>
              subcodecs.get(typeTag) match {
                case Some(subDec) =>
                  subDec.decode(x) // no need to augment error results since we're not moving downward
                case None =>
                  Left(DecodeError.InvalidValue(jTypeTag, Path("type")))
              }
            case Some(other) =>
              Left(DecodeError.InvalidType(expected = JString, got = other.jsonType, Path("type")))
            case None =>
              Left(DecodeError.MissingField("type"))
          }
        case other =>
          Left(DecodeError.InvalidType(expected = JObject, got = other.jsonType))
      }
    }
  }
}
