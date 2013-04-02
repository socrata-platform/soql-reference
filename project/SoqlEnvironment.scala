import sbt._
import Keys._

object SoqlEnvironment{
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ Seq(
    libraryDependencies += "com.ibm.icu" % "icu4j" % "49.1"
  )
}
