package com.socrata.soql.environment

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder}

case class ScopedResourceName[+RNS](scope: RNS, name: ResourceName)

object ScopedResourceName {
  implicit def encode[RNS: JsonEncode] = AutomaticJsonEncodeBuilder[ScopedResourceName[RNS]]
  implicit def decode[RNS: JsonDecode] = AutomaticJsonDecodeBuilder[ScopedResourceName[RNS]]
}
