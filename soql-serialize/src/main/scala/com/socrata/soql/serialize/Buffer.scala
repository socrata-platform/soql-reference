package com.socrata.soql.serialize

import java.io.{IOException, ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}

import com.google.protobuf.{CodedOutputStream, CodedInputStream}

import com.socrata.soql.environment.{ResourceName, TypeName}

sealed trait Version
object Version {
  // Things that care about version-evolvability will match on these;
  // delete them when they're obsolete in order to find places that
  // are still using them!
  case object V3 extends Version
  case object V4 extends Version
  case object V5 extends Version

  val current = V5
}

class WriteBuffer private (val version: Version) {
  private[serialize] val strings = new StringDictionary
  private val rawData = new VisibleByteArrayOutputStream
  private[serialize] val data = CodedOutputStream.newInstance(rawData)

  def write[T](t: T)(implicit ev: Writable[T]): this.type = {
    ev.writeTo(this, t)
    this
  }

  private def writeTo(stream: CodedOutputStream): Unit = {
    val tag = version match {
      case Version.V3 => 3
      case Version.V4 => 4
      case Version.V5 => 5
    }
    stream.writeUInt32NoTag(tag)
    strings.writeTo(stream)
    data.flush()
    stream.writeRawBytes(rawData.asByteBuffer)
  }

  private def writeTo(stream: OutputStream): Unit = {
    val cos = CodedOutputStream.newInstance(stream)
    writeTo(cos)
    cos.flush()
  }
}

object WriteBuffer {
  def asBytes[T : Writable](t: T, version: Version = Version.current): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    write(baos, t, version)
    baos.toByteArray
  }

  def write[T : Writable](os: OutputStream, t: T, version: Version = Version.current): Unit = {
    new WriteBuffer(version).write(t).writeTo(os)
  }
}

class ReadBuffer private (stream: CodedInputStream) {
  private def this(is: InputStream) = this(CodedInputStream.newInstance(is))
  private def this(bytes: Array[Byte]) = this(new ByteArrayInputStream(bytes))

  val version =
    stream.readUInt32() match {
      case other@(0|1|2) =>
        throw new IOException(s"Obsolete version: $other")
      case 3 => Version.V3
      case 4 => Version.V4
      case 5 => Version.V5
      case other =>
        throw new IOException(s"Invalid version: $other")
    }

  private[serialize] val strings = StringDictionary.readFrom(stream)
  private[serialize] val data = stream

  def read[T]()(implicit ev: Readable[T]) = ev.readFrom(this)
}

object ReadBuffer {
  def read[T : Readable](stream: CodedInputStream): T = new ReadBuffer(stream).read[T]()
  def read[T : Readable](stream: InputStream): T = new ReadBuffer(stream).read[T]()
  def read[T : Readable](stream: Array[Byte]): T = new ReadBuffer(stream).read[T]()
}
