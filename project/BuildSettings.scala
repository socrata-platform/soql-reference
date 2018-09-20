import sbt._
import Keys._

import com.socrata.cloudbeessbt.SocrataCloudbeesSbt
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ SocrataCloudbeesSbt.socrataBuildSettings ++ Seq(
    scalaVersion := "2.11.7",
    crossScalaVersions := Seq("2.10.4", scalaVersion.value)
  )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] = buildSettings ++ SocrataCloudbeesSbt.socrataProjectSettings(assembly) ++ Seq(
    // Haven't made a stable release of this yet
    previousArtifact <<= (scalaBinaryVersion,name) { (sv,name) => None /*Some("com.socrata" % (name + "_" + sv) % "0.1.0")*/ },
    testOptions in Test ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
    ),
    scalacOptions += "-language:implicitConversions",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
      "org.scalatest" %% "scalatest" % "2.2.0" % "test"
    ),
    libraryDependencies <++=(scalaVersion) {
      case "2.10.4" => Seq.empty
      case _ => Seq(
        "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
        "org.scala-lang.modules" %% "scala-xml"                % "1.0.3"
      )
    }
  )

  val slf4jVersion = "1.7.5"
}
