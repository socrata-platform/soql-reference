import sbt._
import Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaPreviousArtifacts

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Seq(
    organization := "com.socrata",
    scalaVersion := "2.12.10",
    crossScalaVersions := Seq("2.10.4", "2.11.12", scalaVersion.value),
    externalResolvers ++= Seq("socrata artifactory" at "https://repo.socrata.com/artifactory/libs-release")
  )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] = buildSettings ++ Seq(
    // Haven't made a stable release of this yet
    mimaPreviousArtifacts := Set(/* "com.socrata" % (name.value + "_" + scalaBinaryVersion.value) % "0.1.0" */),
    testOptions in Test ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
    ),
    scalacOptions ++= Seq("-language:implicitConversions", "-feature", "-deprecation"),
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
      "org.scalatest" %% "scalatest" % "3.0.0" % "test"
    ),
    libraryDependencies ++= {
      scalaVersion.value match {
        case "2.10.4" => Seq.empty
        case _ => Seq(
          "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.1",
          "org.scala-lang.modules" %% "scala-xml"                % "1.2.0"
        )
      }
    }
  )

  val slf4jVersion = "1.7.5"
}
