package com.socrata.soql.serialize

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

private class VisibleByteArrayOutputStream extends ByteArrayOutputStream {
  def asByteBuffer =
    ByteBuffer.wrap(buf, 0, count)
}
