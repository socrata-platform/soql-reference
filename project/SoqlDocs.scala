import sbt._
import Keys._

object SoqlDocs {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-docs",
    publish/skip := true,
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion,
      "com.rojoma" %% "rojoma-json-v3" % "3.13.0",
      "com.rojoma" %% "simple-arm-v2" % "2.3.3",
    )
  )
}
