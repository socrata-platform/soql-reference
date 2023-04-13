package com.socrata.soql.collection

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}

object OrderedMapHelper {
  def jsonEncode[K: JsonEncode, V: JsonEncode]: JsonEncode[OrderedMap[K, V]] =
    new JsonEncode[OrderedMap[K, V]] {
      def encode(v: OrderedMap[K, V]) =
        JsonEncode.toJValue(v.toSeq)
    }

  def jsonDecode[K: JsonDecode, V: JsonDecode]: JsonDecode[OrderedMap[K, V]] =
    new JsonDecode[OrderedMap[K, V]] {
      def decode(v: JValue) =
        JsonDecode.fromJValue[Seq[(K, V)]](v).map { kv => OrderedMap() ++ kv }
    }
}
