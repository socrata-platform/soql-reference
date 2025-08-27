import sbt._
import Keys._

object SoqlTypes {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    name := "soql-types",
    libraryDependencies ++=
      Seq(
        // Only used by serialization
        "com.google.guava"    % "guava"                    % "18.0",
        "com.google.protobuf" % "protobuf-java"            % "2.4.1" % "optional",
        "com.rojoma"         %% "rojoma-json-v3"           % "[3.9.1,4.0.0)",
        "com.socrata"        %% "socrata-thirdparty-utils" % "5.1.0",
        "com.vividsolutions"  % "jts"                      % "1.13",
        "commons-io"          % "commons-io"               % "2.5",
        "joda-time"           % "joda-time"                % "2.1",
        "org.joda"            % "joda-convert"             % "1.2",
        "com.socrata"        %% "prettyprinter"            % "0.1.1",
        "org.scalacheck"     %% "scalacheck"               % "1.14.0" % "test"
      )
  )
}
