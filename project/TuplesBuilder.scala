import java.io.{FileOutputStream, BufferedOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import sbt._

import com.rojoma.simplearm.v2._

object TuplesBuilder {
  val N = 22

  def write(dir: File): Seq[File] = {
    save(dir, "WritableTuples.scala", buildWrite)
  }

  def read(dir: File): Seq[File] = {
    save(dir, "ReadableTuples.scala", buildRead)
  }

  private def save(dir: File, name: String, contents: String): Seq[File] = {
    dir.mkdirs()
    val target = dir / name

    for {
      fos <- managed(new FileOutputStream(target))
      bos <- managed(new BufferedOutputStream(fos))
      osw <- managed(new OutputStreamWriter(bos, StandardCharsets.UTF_8))
    } {
      osw.write(contents)
    }

    Seq(target)
  }

  def buildWrite: String = {
    val sb = new StringBuilder

    sb.append("package com.socrata.soql.serialize\n")
    sb.append("package `-impl`\n")
    sb.append("\n")

    sb.append("trait WritableTuples { this: Writable.type =>\n")

    for(i <- 2 to N) {
      val typeParams = (1 to i).map { j => s"T$j : Writable" }.mkString("[", ", ", "]")
      val tuple = (1 to i).map { j => s"T$j" }.mkString("(", ", ", ")")
      sb.append("  implicit def tuple").append(i).append(typeParams).append(": Writable[").append(tuple).append("] = new Writable[").append(tuple).append("] {\n")
      sb.append("    def writeTo(buffer: WriteBuffer, tuple: ").append(tuple).append("): Unit = {\n")
      for(j <- 1 to i) {
        sb.append(s"      buffer.write(tuple._$j)\n")
      }
      sb.append("    }\n")
      sb.append("  }\n")
    }

    sb.append("}\n")

    sb.toString
  }

  def buildRead: String = {
    val sb = new StringBuilder

    sb.append("package com.socrata.soql.serialize\n")
    sb.append("package `-impl`\n")
    sb.append("\n")

    sb.append("trait ReadableTuples { this: Readable.type =>\n")

    for(i <- 2 to N) {
      val typeParams = (1 to i).map { j => s"T$j : Readable" }.mkString("[", ", ", "]")
      val tuple = (1 to i).map { j => s"T$j" }.mkString("(", ", ", ")")
      sb.append("  implicit def tuple").append(i).append(typeParams).append(": Readable[").append(tuple).append("] = new Readable[").append(tuple).append("] {\n")
      sb.append("    def readFrom(buffer: ReadBuffer): ").append(tuple).append(" = {\n")
      sb.append("      (\n")
      for(j <- 1 to i) {
        sb.append(s"        buffer.read[T$j]()")
        if(j != i) sb.append(',')
        sb.append('\n')
      }
      sb.append("      )\n")
      sb.append("    }\n")
      sb.append("  }\n")
    }

    sb.append("}\n")

    sb.toString
  }
}
