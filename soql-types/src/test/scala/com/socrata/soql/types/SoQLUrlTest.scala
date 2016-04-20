package com.socrata.soql.types

import org.scalatest.{MustMatchers, WordSpec}


class SoQLUrlTest  extends WordSpec with MustMatchers {

  val url = "http://www.socrata.com"
  val description = "Home Site"

  "SoQLUrl" should {
    "be able to cast from json" in {
      SoQLUrl.parseUrl(s"""{ "url": "$url", "description": "$description" }""") must be
        (Some(SoQLUrl(Some(url), Some(description))))
    }

    "be able to cast from uri" in {
      SoQLUrl.parseUrl(url) must be (Some(SoQLUrl(Some(url), None)))
    }

    "be able to cast from html tag" in {
      SoQLUrl.parseUrl(s"""<a href="$url">$description</a>""") must be (Some(SoQLUrl(Some(url), Some(description))))
    }

    "do not parse random text" in {
      SoQLUrl.parseUrl("random text") must be (None)
    }
  }
}
