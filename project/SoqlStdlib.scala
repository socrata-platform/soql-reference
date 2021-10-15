import sbt._
import Keys._

object SoqlStdlib {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-stdlib",
    libraryDependencies ++= Seq(
      "com.rojoma" %% "rojoma-json-v3" % "3.13.0",
      "com.rojoma" %% "simple-arm-v2" % "2.3.1"
    ),
    externalResolvers += "Socrata Jcenter Mirror" at "https://repo.socrata.com/artifactory/jcenter"
  )
}
