package com.socrata.soql.util

import scala.reflect.ClassTag

import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError, Path}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, SimpleHierarchyCodecBuilder, InternalTag}

// Ok, this is a little ugly, but here's what it's doing:
//
// We want our user-actionable error codes to have a common format.  This format will be:
//
// {
//   "code": "some-identifier-for-machines",
//   "data": { .. error-specific data .. }
//   "message": "some explanation for humans",
//   "scope": relevant-scope // optional, not all errors are scoped
// }
//
// And we want to be able to treat errors that are defined in
// different places (e.g., here in soql-reference or in the various
// services) uniformly.
//
// So, first we'll define a type ("EncodedError") that represents this
// custom format, and provide an easy way to build codecs for types
// that can be turned into this encoded error via the SoQLErrorEncode
// and SoQLErrorDecode traits.
//
// Then, we'll want to collect them together into groups.  For
// example, this project defines errors in SoQLAnalyzerError.scala;
// other projects might define errors elsewhere.  But we want to say
// "I have an error that might come from any of these projects", so
//
// sealed abstract class UpstreamError
// case class SoQLError(err: soqlanalyzer.Error[MyScope]) extends UpstreamError
// case class ExecutionError(err: soqlpg.Error[MyScope]) extends UpstreamError
// object UpstreamError {
//   val decode: JsonDecode[UpstreamError] = (
//     soqlanalyzer.Error.codecs[MyScope].toDecode.map(SoQLError(_)) ++
//     soqlpg.Error.codecs[MyScope].toDecode.map(ExecutionError(_))
//   ).build
// }
//
// ..and then we have a json decode that can decode multiple types of error.
//
// A fair amount amount of the apparent complexity here is from the
// need to hide the types of things under the Maps (so for example
// Branch[T] passes through an intermediate type but that's completely
// hidden in the API.
case class EncodedError(
  code: String,
  data: JValue,
  message: String,
  scope: Option[JValue]
)

trait SoQLErrorEncode[T] {
  val code: String

  // This will generally be either a call to result/2 if this error
  // type is unscoped, or a call to result/3 with an intermediate
  // value representing the non-scope data fields.
  def encode(err: T): EncodedError

  protected final def result[Data: JsonEncode, RNS: JsonEncode](
    data: Data,
    message: String,
    scope: RNS
  ): EncodedError = {
    EncodedError(
      code,
      JsonEncode.toJValue(data),
      message,
      Some(JsonEncode.toJValue(scope))
    )
  }

  protected final def result[Data: JsonEncode](
    data: Data,
    message: String
  ): EncodedError = {
    EncodedError(code, JsonEncode.toJValue(data), message, None)
  }
}

trait SoQLErrorDecode[T] {
  val code: String

  // Depending on the needs of the type, this will either just be a
  // call to `data` or something that looks like
  //
  // for {
  //   decodedData <- data[Intermediate](err)
  //   decodedScope <- scope[RNS](err)
  // } yield { .. construct a value from decodedData and decodedScope ... }
  def decode(err: EncodedError): Either[DecodeError, T]

  protected final def data[Data: JsonDecode](err: EncodedError): Either[DecodeError, Data] =
    JsonDecode.fromJValue(err.data).left.map(_.prefix("data"))

  protected final def scope[RNS: JsonDecode](err: EncodedError): Either[DecodeError, RNS] = {
    err.scope match {
      case Some(scope) => JsonDecode.fromJValue(scope).left.map(_.prefix("scope"))
      case None => Left(DecodeError.MissingField("scope"))
    }
  }
}

abstract class SoQLErrorCodec[T](override val code: String) extends SoQLErrorEncode[T] with SoQLErrorDecode[T]

object SoQLErrorCodec {
  private val encodedErrorCodec = AutomaticJsonCodecBuilder[EncodedError]

  // "Given a SoQLErrorEncode[T], you can produce a JsonEncode for that same T"
  implicit def jsonEncode[T](implicit see: SoQLErrorEncode[T]): JsonEncode[T] =
    new JsonEncode[T] {
      def encode(v: T) = {
        encodedErrorCodec.encode(see.encode(v))
      }
    }

  // "Given a SoQLErrorDecode[T], you can produce a JsonDecode for that same T"
  implicit def jsonDecode[T](implicit sed: SoQLErrorDecode[T]): JsonDecode[T] =
    new JsonDecode[T] {
      def decode(v: JValue) = {
        encodedErrorCodec.decode(v).flatMap { intermediate =>
          if(intermediate.code != sed.code) {
            Left(DecodeError.InvalidValue(JString(intermediate.code), Path("code")))
          } else {
            sed.decode(intermediate)
          }
        }
      }
    }

  // This trait hides some sometype of T
  private trait Branch[T <: AnyRef] {
    def addToCodec(builder: SimpleHierarchyCodecBuilder[T]): SimpleHierarchyCodecBuilder[T]
    def mappable: MappedBranch[T]
  }

  // Like SimpleHierarchyCodecBuilder, only it can be split off into a
  // decode-only variant (which can then be mapped, see
  // ErrorDecodes below)
  class ErrorCodecs[T <: AnyRef] private (private val branches: List[Branch[T]], private val codes: Set[String]) {
    def this() = this(Nil, Set.empty)

    def branch[U <: T : ClassTag](implicit encode: SoQLErrorEncode[U], decode: SoQLErrorDecode[U]): ErrorCodecs[T] = {
      require(encode.code == decode.code)
      require(!codes.contains(encode.code))

      val branch = new Branch[T] {
        private val uDecode: JsonDecode[U] = jsonDecode(decode)

        private val code = decode.code

        override def addToCodec(builder: SimpleHierarchyCodecBuilder[T]) = {
          builder.branch[U](code)(jsonEncode(encode), uDecode, implicitly)
        }

        override def mappable =
          MappedBranch[T](
            code,
            new JsonDecode[T] {
              override def decode(v: JValue) = uDecode.decode(v)
            }
          )
      }

      new ErrorCodecs[T](branch :: branches, codes + encode.code)
    }

    def toDecode: ErrorDecodes[T] =
      new ErrorDecodes(branches.map(_.mappable), codes)

    def build: JsonEncode[T] with JsonDecode[T] =
      branches.reverse.foldLeft(SimpleHierarchyCodecBuilder[T](InternalTag("code", removeTagForSubcodec = false))) { (builder, branch) =>
        branch.addToCodec(builder)
      }.build
  }

  // Like Branch above, this hides an intermediate type
  private trait MappedBranch[+T] {
    def addToDecode[U >: T](d: SimplerHierarchyDecodeBuilder[U]): SimplerHierarchyDecodeBuilder[U]
    def map[U](f: T => U): MappedBranch[U]
  }

  private object MappedBranch {
    private class MappedBranchImpl[T, U](code: String, underlying: JsonDecode[T], f: T => U) extends MappedBranch[U] {
      override def addToDecode[V >: U](builder: SimplerHierarchyDecodeBuilder[V]): SimplerHierarchyDecodeBuilder[V] =
        builder.branch(code)(
          new JsonDecode[U] {
            override def decode(v: JValue) = underlying.decode(v).map(f)
          }
        )

      def map[V](f2: U => V) = new MappedBranchImpl[T, V](code, underlying, f2 compose f)
    }

    def apply[T <: AnyRef](code: String, jDecode: JsonDecode[T]): MappedBranch[T] =
      new MappedBranchImpl[T, T](code, jDecode, identity)
  }

  class ErrorDecodes[T] private[SoQLErrorCodec] (private val branches: List[MappedBranch[T]], private val codes: Set[String]) {
    def ++[U >: T](that: ErrorDecodes[U]): ErrorDecodes[U] = {
      require(this.codes.intersect(that.codes).isEmpty)
      new ErrorDecodes(that.branches ++ this.branches, this.codes ++ that.codes)
    }

    def map[U](f: T => U): ErrorDecodes[U] =
      new ErrorDecodes[U](branches.map(_.map(f)), codes)

    def build: JsonDecode[T] =
      branches.reverse.foldLeft(SimplerHierarchyDecodeBuilder[T]("code")) { (builder, branch) =>
        branch.addToDecode(builder)
      }.build
  }
}
