import sbt._
import Keys._

import com.socrata.socratasbt.SocrataSbt._

object SoqlEnvironment{
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings() ++ BuildSettings.overrides ++ Seq(
    libraryDependencies += "com.ibm.icu" % "icu4j" % "49.1"
  )
}
