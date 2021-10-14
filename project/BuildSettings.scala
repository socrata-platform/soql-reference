import sbt._
import Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaPreviousArtifacts

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Seq(
    organization := "com.socrata",
    scalaVersion := "2.13.6",
    crossScalaVersions := Seq("2.12.10", scalaVersion.value),
    externalResolvers ++= Seq("socrata artifactory" at "https://repo.socrata.com/artifactory/libs-release")
  )

  def projectSettings(assembly: Boolean = false): Seq[Setting[_]] = buildSettings ++ Seq(
    // Haven't made a stable release of this yet
    mimaPreviousArtifacts := Set(/* "com.socrata" % (name.value + "_" + scalaBinaryVersion.value) % "0.1.0" */),
    testOptions in Test ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
    ),
    scalacOptions ++= Seq(
      "-language:implicitConversions",
      "-deprecation",
      "-feature"
    ),
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
      "org.scalatest" %% "scalatest" % "3.0.8" % "test"
    ),
    libraryDependencies ++= {
      scalaVersion.value match {
        case _ => Seq(
          "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.0",
          "org.scala-lang.modules" %% "scala-xml"                % "2.0.1",
          "org.scala-lang.modules" %% "scala-collection-compat"  % "2.5.0"
        )
      }
    }
  )

  val slf4jVersion = "1.7.5"
}
