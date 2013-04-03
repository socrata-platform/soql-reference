import sbt._
import Keys._

import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Defaults.defaultSettings ++ mimaDefaultSettings ++ Seq(
    organization := "com.socrata",
    version := "0.0.15",
    // Haven't made a stable release of this yet
    // previousArtifact <<= (scalaBinaryVersion,name) { sv => Some("com.socrata" % (name + "_" + sv) % "0.1.0") }
    scalaVersion := "2.10.0",
    crossScalaVersions := Seq("2.9.2", "2.10.0"),
    testOptions in Test ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-oFD")
    ),
    scalacOptions <++= (scalaVersion) map {
      case s if s.startsWith("2.9.") => Seq("-encoding", "UTF-8", "-g:vars", "-unchecked", "-deprecation")
      case s if s.startsWith("2.10.") => Seq("-encoding", "UTF-8", "-g:vars", "-deprecation", "-feature", "-language:implicitConversions")
    },
    javacOptions ++= Seq("-encoding", "UTF-8", "-g", "-Xlint:unchecked", "-Xlint:deprecation", "-Xmaxwarns", "999999"),
    ivyXML := // com.rojoma and com.socrata have binary compat guarantees
      <dependencies>
        <conflict org="com.socrata" manager="latest-compatible"/>
        <conflict org="com.rojoma" manager="latest-compatible"/>
      </dependencies>,
    libraryDependencies <++= (scalaVersion) { sv =>
      Seq(
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
        "org.scalatest" %% "scalatest" % scalaTestVersion(sv) % "test"
      )
    },
    unmanagedSourceDirectories in Compile <+= (scalaVersion, scalaSource in Compile) { (sv, commonSource) => commonSource.getParentFile / scalaDirFor(sv) },
    unmanagedSourceDirectories in Test <+= (scalaVersion, scalaSource in Test) { (sv, commonSource) => commonSource.getParentFile / scalaDirFor(sv) }
  )

  private def scalaDirFor(scalaVersion: String): String = {
    val MajorMinor = """(\d+\.\d+)\..*""".r
    scalaVersion match {
      case MajorMinor(mm) => "scala-" + mm
      case _ => sys.error("Unable to find major/minor Scala version in " + scalaVersion)
    }
  }

  val slf4jVersion = "1.7.5"

  def scalaTestVersion(sv: String) = sv match {
    case s if s.startsWith("2.8.") => "1.8"
    case _ => "1.9.1"
  }
}
