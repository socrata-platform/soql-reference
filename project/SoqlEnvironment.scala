import sbt._
import Keys._

object SoqlEnvironment{
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    crossScalaVersions += "2.8.1",
    libraryDependencies += "com.ibm.icu" % "icu4j" % "49.1"
  )
}
