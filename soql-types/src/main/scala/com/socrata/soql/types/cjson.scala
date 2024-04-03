package com.socrata.soql.types

import com.rojoma.json.v3.ast.{JValue, JNull}
import com.rojoma.json.v3.codec.DecodeError

// A read rep either reads a value of type T or a null.  Thus, it
// returns an Option[T].
trait CJsonReadRep[+T] {
  final def fromJValue(v: JValue): Either[DecodeError, Option[T]] =
    v match {
      case JNull => Right(None)
      case other => fromJValueImpl(other) match {
        case Right(t) => Right(Some(t))
        case Left(CJsonReadRep.InvalidTypeError(err)) =>
          Left(DecodeError.join(Seq(err, DecodeError.InvalidType(expected = JNull, got = other.jsonType))))
        case Left(err) =>
          Left(err)
      }
    }

  final def fromJValueRequired(v: JValue): Either[DecodeError, T] =
    fromJValueImpl(v)

  protected def fromJValueImpl(v: JValue): Either[DecodeError, T]
}

object CJsonReadRep {
  private object InvalidTypeError {
    def unapply(e: DecodeError): Option[DecodeError] =
      e match {
        case _ : DecodeError.InvalidType => Some(e)
        case DecodeError.Multiple(errs) if errs.exists(unapply(_).isDefined) => Some(e)
        case _ => None
      }
  }
}

// A write rep can write a T (or an optional T).
trait CJsonWriteRep[-T] {
  def toJValue(v: T): JValue
  final def toJValueOpt(v: Option[T]): JValue =
    v match {
      case Some(v) => toJValue(v)
      case None => JNull
    }
}

// A read rep can be "erased", which means it turns from a
// CJsonReadRep[SoQLText] (for example) into an
// ErasedCJsonReadRep[SoQLValue] which will fail with a decoding error
// if it reads anything other than a SoQLNull or a SoQLText.
trait ErasedCJsonReadRep[+T] {
  // Because of the quirk where "null" is a distinct type, this does
  // _not_ return an Option[T].  It just returns a SoQLNull if that's
  // what it read.  This is usually what you want if you're using an
  // erased rep.
  def fromJValue(v: JValue): Either[DecodeError, T]

  // ..but if it's not, there's this.
  def fromJValueOpt(v: JValue): Either[DecodeError, Option[T]]
}

// Similarly, a CJsonWriteRep[T] can be erased, which means it turns
// from a CJsonWriteRep[SoQLText] (for example) into a CJsonWriteRep
// which throws an exception if it's given an incorrectly-typed value.
trait ErasedCJsonWriteRep[-T] {
  // This accepts either the real T or SoQLNull and writes the result.
  def toJValue(v: T): JValue

  // This accepts _only_ the real T and writes the result.  Passing
  // Some(SoQLNull) will throw an exception!
  def toJValueOpt(v: Option[T]): JValue
}

// Convenience trait that just inherits both directions.
trait ErasedCJsonRep[T] extends ErasedCJsonReadRep[T] with ErasedCJsonWriteRep[T]

// Trait which allows a CJsonWriteRep to be erased
trait AsErasedCJsonWriteRep[T, U >: T] { self: CJsonWriteRep[T] =>
  protected def downcast(v: U): Option[T]
  protected def isNull(v: U): Boolean

  protected trait AnyWriteRep extends ErasedCJsonWriteRep[U] {
    override final def toJValue(u: U) =
      if(isNull(u)) JNull
      else downcast(u) match {
        case Some(ok) => self.toJValue(ok)
        case None => throw new IllegalArgumentException("Invalid value")
      }

    override final def toJValueOpt(uOpt: Option[U]) =
      uOpt match {
        case Some(u) =>
          downcast(u) match {
            case Some(ok) => self.toJValue(ok)
            case None => throw new IllegalArgumentException("Invalid value")
          }
        case None =>
          JNull
      }
  }

  def asErasedCJsonWriteRep: ErasedCJsonWriteRep[U] = new AnyWriteRep {}
}

// Trait which allows a CJsonReadRep to be erased
trait AsErasedCJsonReadRep[T, U >: T] { self: CJsonReadRep[T] =>
  protected def mkNull: U

  protected trait AnyReadRep extends ErasedCJsonReadRep[U] {
    override final def fromJValue(j: JValue) =
      self.fromJValue(j) match {
        case Right(Some(decoded)) => Right(decoded)
        case Right(None) => Right(mkNull)
        case Left(err) => Left(err)
      }

    override final def fromJValueOpt(j: JValue) =
      self.fromJValue(j)
  }

  def asErasedCJsonReadRep: ErasedCJsonReadRep[U] = new AnyReadRep {}
}

trait AsErasedCJsonRep[T, U >: T] extends AsErasedCJsonReadRep[T, U] with AsErasedCJsonWriteRep[T, U] { self: CJsonReadRep[T] with CJsonWriteRep[T] =>
  def asErasedCJsonRep: ErasedCJsonRep[U] =
    new ErasedCJsonRep[U] with AnyWriteRep with AnyReadRep
}

abstract class CJsonRep[T, U >: T] extends CJsonReadRep[T] with CJsonWriteRep[T] with AsErasedCJsonRep[T, U]
