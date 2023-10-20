package com.socrata.soql.util

import scala.language.existentials
import scala.reflect.ClassTag

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec._

class ErrorHierarchyEncodeBuilder[Root <: AnyRef] private (subcodecs: Map[String, JsonEncode[_ <: Root]], classes: Map[Class[_], String]) {
  def this() = this(Map.empty, Map.empty)

  def branch[T <: Root](implicit enc: SoQLErrorEncode[T], mfst: ClassTag[T]) = {
    val name = enc.code
    val cls = mfst.runtimeClass
    if(subcodecs contains name) throw new IllegalArgumentException("Already defined a encoder for branch " + name)
    if(classes.contains(cls)) throw new IllegalArgumentException("Already defined a encoder for class " + cls)
    new ErrorHierarchyEncodeBuilder[Root](subcodecs + (name -> SoQLErrorCodec.jsonEncode(enc)), classes + (cls -> name))
  }

  private def encFor(x: Root) =
    classes.get(x.getClass) match {
      case Some(name) => (name, subcodecs(name))
      case None => throw new IllegalArgumentException("No encoder defined for " + x.getClass)
    }

  def build: JsonEncode[Root] = {
    if(subcodecs.isEmpty) throw new IllegalStateException("No branches defined")
    new JsonEncode[Root] {
      def encode(x: Root): JValue = {
        val (name, subenc) = encFor(x)
        subenc.asInstanceOf[JsonEncode[Root]].encode(x)
      }
    }
  }
}

object ErrorHierarchyEncodeBuilder {
  def apply[Root <: AnyRef] = new ErrorHierarchyEncodeBuilder[Root](Map.empty, Map.empty)
}
