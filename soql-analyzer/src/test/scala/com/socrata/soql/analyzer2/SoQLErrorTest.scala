package com.socrata.soql.analyzer2

import scala.util.parsing.input.NoPosition

import org.scalatest.{FunSuite, MustMatchers}

import com.rojoma.json.v3.util.{SimpleHierarchyCodecBuilder, InternalTag}

import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.Source
import com.socrata.soql.util.{SoQLErrorCodec, EncodedError}

class SoQLErrorTest extends FunSuite with MustMatchers with TestHelper {
  lazy val codecs = SoQLError.errorCodecs[Int, SoQLError[Int]]()
  lazy val jsonCodecs = codecs.build

  sealed trait OtherErrors
  case class OtherError() extends OtherErrors
  object OtherError {
    implicit val codec = new SoQLErrorCodec[OtherError]("other-error") {
      def encode(err: OtherError) = result("haha", "this is a message")
      def decode(v: EncodedError) = Right(OtherError())
    }
  }
  object OtherErrors {
    def errorCodecs[T >: OtherErrors <: AnyRef](
      codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
    ): SoQLErrorCodec.ErrorCodecs[T] =
      codecs.branch[OtherError]
  }

  sealed abstract class MultiError
  case class SomeSoQLError(err: SoQLError[Int]) extends MultiError
  case class SomeOtherError(err: OtherErrors) extends MultiError

  lazy val decode =
    (
      codecs.toDecode.map[MultiError](SomeSoQLError(_)) ++
        OtherErrors.errorCodecs().toDecode.map[MultiError](SomeOtherError(_))
    ).build

  test("No colliding tags") {
    jsonCodecs // just force evaluation
  }

  test("Can be decoded as part of a bundle") {
    val tf = tableFinder(
      (0, "a") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "q") -> Q(0, "a", "haha invalid soql")
    )

    val Left(err@ParserError.ExpectedToken(_, _, _)) = tf.findTables(0, rn("q"))
    val encoded = jsonCodecs.encode(err)

    val Right(SomeSoQLError(err2@ParserError.ExpectedToken(_, _, _))) = decode.decode(encoded)

    // positions, annoyingly, don't round-trip through json, so make
    // sure their observable things are the same
    err2.source.position.line must equal (err.source.position.line)
    err2.source.position.column must equal (err.source.position.column)
    err2.source.position.longString must equal (err.source.position.longString)

    err2.copy(source = err2.source.withPosition(NoPosition)) must equal (err.copy(source = err.source.withPosition(NoPosition)))
  }
}
