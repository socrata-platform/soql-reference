import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object SoqlAnalysis {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings() ++ BuildSettings.overrides ++ Seq(
    libraryDependencies <++= (slf4jVersion) { slf4j =>
      Seq(
        "org.slf4j" % "slf4j-simple" % slf4j % "test"
      )
    },
    fork in (Test, run) := true,
    javaOptions in (Test, run) := Seq("-Dorg.slf4j.simpleLogger.log.com.socrata=DEBUG")
  )
}
