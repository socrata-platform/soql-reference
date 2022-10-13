package com.socrata.soql.analyzer2.serialization

import java.io.IOException

import com.google.protobuf.{CodedOutputStream, CodedInputStream}

import com.socrata.soql.environment.{ResourceName, TypeName}

sealed trait Version
object Version {
  // Things that care about version-evolvability will match on these;
  // delete them when they're obsolete in order to find places that
  // are still using them!
  case object V0 extends Version
}

class WriteBuffer(val version: Version = Version.V0) {
  private[serialization] val strings = new StringDictionary
  private val rawData = new VisibleByteArrayOutputStream
  private[serialization] val data = CodedOutputStream.newInstance(rawData)

  def write[T](t: T)(implicit ev: Writable[T]) = ev.writeTo(this, t)

  def writeTo(stream: CodedOutputStream): Unit = {
    val tag = version match {
      case Version.V0 => 0
    }
    stream.writeUInt32NoTag(tag)
    strings.writeTo(stream)
    data.flush()
    stream.writeRawBytes(rawData.asByteBuffer)
  }
}

class ReadBuffer(stream: CodedInputStream) {
  val version =
    stream.readUInt32() match {
      case 0 =>
        Version.V0
      case other =>
        throw new IOException("Invalid version: {}")
    }

  private[serialization] val strings = StringDictionary.readFrom(stream)
  private[serialization] val data = stream

  def read[T]()(implicit ev: Readable[T]) = ev.readFrom(this)
}
