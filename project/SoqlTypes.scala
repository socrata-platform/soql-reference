import sbt._
import Keys._

object SoqlTypes {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    crossScalaVersions += "2.8.1",
    resolvers ++= oldRojomaJsonRepo(scalaVersion.value),
    libraryDependencies ++=
      Seq(
        // Only used by serialization
        "com.google.protobuf" % "protobuf-java"            % "2.4.1" % "optional",
        "com.rojoma"         %% "rojoma-json"              % rojomaJsonVersion(scalaVersion.value),
        "com.socrata"        %% "socrata-thirdparty-utils" % "2.6.2",
        "com.vividsolutions"  % "jts"                      % "1.13",
        "commons-io"          % "commons-io"               % "1.4",
        "joda-time"           % "joda-time"                % "2.1",
        "org.bouncycastle"    % "bcprov-jdk15on"           % "1.48",
        "org.joda"            % "joda-convert"             % "1.2",
        "org.scalacheck"     %% "scalacheck"               % scalaCheckVersion(scalaVersion.value) % "test"
      )
  )

  def oldRojomaJsonRepo(scalaVersion: String) = scalaVersion match {
    case "2.8.1" => Seq("rjmac maven" at "https://rjmac.github.io/maven/releases")
    case _ => Nil
  }

  def rojomaJsonVersion(scalaVersion: String) = scalaVersion match {
    case "2.8.1" => "1.4.4"
    case _ => "[2.0.0,3.0.0)"
  }

  def scalaCheckVersion(scalaVersion: String) = scalaVersion match {
    case "2.8.1" => "1.8"
    case _ => "1.10.0"
  }
}
