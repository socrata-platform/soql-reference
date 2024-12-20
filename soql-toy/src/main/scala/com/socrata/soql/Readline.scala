package com.socrata.soql

import org.jline.reader.{LineReaderBuilder, EndOfFileException};

object Readline {
  val reader = LineReaderBuilder.builder().build()

  def apply(prompt: String = "> "): Option[String] = {
    try {
      Option(reader.readLine(prompt))
    } catch {
      case e: EndOfFileException =>
        None
    }
  }
}
