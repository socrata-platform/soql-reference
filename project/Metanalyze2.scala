import sbt._
import Keys._

object Metanalyze2 {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    name := "metanalyze2",
    run/fork := true,
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion,
      "com.rojoma" %% "rojoma-sql-v1" % "0.0.1",
      "com.rojoma" %% "rojoma-sql-v1-postgresql" % "0.0.1",
    )
  )
}
