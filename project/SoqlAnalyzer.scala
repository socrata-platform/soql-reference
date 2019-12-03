import sbt._
import Keys._

object SoqlAnalyzer {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      // These two are only used by analysis-serialization
      "com.google.protobuf" % "protobuf-java" % "2.4.1" % "optional",
      "net.sf.trove4j" % "trove4j" % "3.0.3" % "optional",

      "org.slf4j" % "slf4j-simple" % BuildSettings.slf4jVersion % "test",
      "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"
    )
  )
}
