import sbt._
import Keys._

object SoqlTypes {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    crossScalaVersions += "2.8.1",
    libraryDependencies <++= (scalaVersion) { sv =>
      Seq(
        "joda-time" % "joda-time" % "2.1",
        "org.joda" % "joda-convert" % "1.2",
        "com.rojoma" %% "rojoma-json" % rojomaJsonVersion(sv),
        "org.bouncycastle" % "bcprov-jdk15on" % "1.48",

        // Only used by serialization
        "com.google.protobuf" % "protobuf-java" % "2.4.1" % "optional",

        "org.scalacheck" %% "scalacheck" % scalaCheckVersion(sv) % "test"
      )
    }
  )

  def rojomaJsonVersion(scalaVersion: String) = scalaVersion match {
    case "2.8.1" => "1.4.4"
    case _ => "[2.0.0,3.0.0)"
  }

  def scalaCheckVersion(scalaVersion: String) = scalaVersion match {
    case "2.8.1" => "1.8"
    case _ => "1.10.0"
  }
}
