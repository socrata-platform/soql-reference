resolvers ++= Seq(
  "socrata releases" at "https://repo.socrata.com/artifactory/libs-release/"
)

organization := "com.socrata"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("org.scoverage" %% "sbt-scoverage" %  "1.1.0")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.11")

libraryDependencies ++= Seq(
  "de.jflex" % "jflex" % "1.4.3",
  "org.apache.ant" % "ant" % "1.8.4"
)
