package com.socrata.soql.toy

import scala.io.StdIn

object Compat {
  def readLine(prompt: String) = StdIn.readLine(prompt)
}
