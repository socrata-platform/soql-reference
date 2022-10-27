package com.socrata.soql.analyzer2.serialization

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

private class VisibleByteArrayOutputStream extends ByteArrayOutputStream {
  def asByteBuffer =
    ByteBuffer.wrap(buf, 0, count)
}
