package com.socrata.soql.util

import scala.language.existentials
import scala.reflect.ClassTag

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec._

private[util] class ErrorHierarchyEncodeBuilder[Root <: AnyRef] private (subcodecs: Map[String, (JsonEncode[_ <: Root], AbstractErrorEncode[_ <: Root])], classes: Map[Class[_], String]) {
  def this() = this(Map.empty, Map.empty)

  private def toAbstract[T](enc: SoQLErrorEncode[T]) = new AbstractErrorEncode[T] {
    override def encodeError(err: T): EncodedError =
      enc.encode(err)
  }

  def branch[T <: Root](implicit enc: SoQLErrorEncode[T], mfst: ClassTag[T]) = {
    val name = enc.code
    val cls = mfst.runtimeClass
    if(subcodecs contains name) throw new IllegalArgumentException("Already defined a encoder for branch " + name)
    if(classes.contains(cls)) throw new IllegalArgumentException("Already defined a encoder for class " + cls)
    new ErrorHierarchyEncodeBuilder[Root](subcodecs + (name -> ((SoQLErrorCodec.jsonEncode(enc), toAbstract(enc)))), classes + (cls -> name))
  }

  private def encFor(x: Root) =
    classes.get(x.getClass) match {
      case Some(name) => subcodecs(name)
      case None => throw new IllegalArgumentException("No encoder defined for " + x.getClass)
    }

  def build: JsonEncode[Root] with AbstractErrorEncode[Root] = {
    if(subcodecs.isEmpty) throw new IllegalStateException("No branches defined")
    new JsonEncode[Root] with AbstractErrorEncode[Root] {
      def encode(x: Root): JValue = {
        val (subenc, _) = encFor(x)
        subenc.asInstanceOf[JsonEncode[Root]].encode(x)
      }

      def encodeError(x: Root): EncodedError = {
        val (_, subenc) = encFor(x)
        subenc.asInstanceOf[AbstractErrorEncode[Root]].encodeError(x)
      }
    }
  }
}
