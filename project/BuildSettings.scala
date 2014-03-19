import sbt._
import Keys._

import com.socrata.cloudbeessbt.SocrataCloudbeesSbt
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ SocrataCloudbeesSbt.socrataBuildSettings ++ Seq(
    scalaVersion := "2.10.2",
    version := "0.0.16"
  )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] = buildSettings ++ SocrataCloudbeesSbt.socrataProjectSettings(assembly) ++ Seq(
    // Haven't made a stable release of this yet
    previousArtifact <<= (scalaBinaryVersion,name) { (sv,name) => None /*Some("com.socrata" % (name + "_" + sv) % "0.1.0")*/ },
    testOptions in Test ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
    ),
    scalacOptions <++= (scalaVersion) map {
      case "2.8.1" => Nil
      case _ => Seq("-language:implicitConversions")
    },
    libraryDependencies <++= (scalaVersion) { sv =>
      Seq(
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
        "org.scalatest" %% "scalatest" % scalaTestVersion(sv) % "test"
      )
    }
  )

  val slf4jVersion = "1.7.5"

  def scalaTestVersion(sv: String) = sv match {
    case "2.8.1" => "1.8"
    case _ => "1.9.1"
  }
}
