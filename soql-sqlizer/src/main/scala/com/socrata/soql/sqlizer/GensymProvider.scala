package com.socrata.soql.sqlizer

import com.socrata.prettyprint.prelude._

class GensymProvider(namespaces: SqlNamespaces[_]) {
  private var counter = 0L

  def next(): Doc[Nothing] = {
    counter += 1
    d"${namespaces.gensymPrefix}${counter}"
  }
}
