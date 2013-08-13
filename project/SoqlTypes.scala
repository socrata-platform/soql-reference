import sbt._
import Keys._

object SoqlTypes {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % "2.1",
      "org.joda" % "joda-convert" % "1.2",
      "com.rojoma" %% "rojoma-json" % "[2.0.0,3.0.0)",
      "org.bouncycastle" % "bcprov-jdk15on" % "1.48",

      // Only used by serialization
      "com.google.protobuf" % "protobuf-java" % "2.4.1" % "optional",

      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
    )
  )
}
