import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object SoqlToy {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings(assembly=true) ++ BuildSettings.overrides ++ Seq(
    libraryDependencies <++= (slf4jVersion) { slf4j =>
      Seq(
        "org.slf4j" % "slf4j-simple" % slf4j
      )
    }
  )
}
