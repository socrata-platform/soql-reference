package com.socrata.soql.util

import scala.reflect.ClassTag

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec._

private[util] class ErrorHierarchyCodecBuilder[Root <: AnyRef] private (enc: ErrorHierarchyEncodeBuilder[Root], dec: ErrorHierarchyDecodeBuilder[Root]) {
  def this() = this(new ErrorHierarchyEncodeBuilder[Root], new ErrorHierarchyDecodeBuilder[Root])

  def branch[T <: Root : SoQLErrorEncode : SoQLErrorDecode : ClassTag] = {
    require(implicitly[SoQLErrorEncode[T]].code == implicitly[SoQLErrorDecode[T]].code)
    new ErrorHierarchyCodecBuilder[Root](enc.branch[T], dec.branch[T])
  }

  def build: JsonEncode[Root] with JsonDecode[Root] = {
    new JsonEncode[Root] with JsonDecode[Root] {
      val e = enc.build
      val d = dec.build
      def encode(x: Root) = e.encode(x)
      def decode(x: JValue) = d.decode(x)
    }
  }
}
