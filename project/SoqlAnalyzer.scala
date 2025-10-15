import sbt._
import Keys._

object SoqlAnalyzer {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-analyzer",
    libraryDependencies ++= Seq(
      // These two are only used by analysis-serialization
      "com.google.protobuf" % "protobuf-java" % "2.6.1" % "optional",
      "net.sf.trove4j" % "trove4j" % "3.0.3" % "optional",

      "com.rojoma" %% "rojoma-json-v3" % "3.13.0",

      "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion % "test",
      "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
    ),
    libraryDependencies ++= {
      if(scalaVersion.value.startsWith("2.13")) Nil
      else Seq(compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full))
    },
    scalacOptions ++= {
      if(scalaVersion.value.startsWith("2.13")) Seq("-Ymacro-annotations")
      else Nil
    },
    scalacOptions ++= Seq("-Werror")
  )
}
