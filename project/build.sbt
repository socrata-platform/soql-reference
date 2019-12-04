externalResolvers ++= Seq(
  "socrata releases" at "https://repo.socrata.com/artifactory/libs-release/",
  Resolver.url(
    "typesafe sbt-plugins",
    url("https://dl.bintray.com/typesafe/sbt-plugins")
  )(Resolver.ivyStylePatterns)
)

organization := "com.socrata"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("org.scoverage" %% "sbt-scoverage" %  "1.6.1")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.6.1")

libraryDependencies ++= Seq(
  "de.jflex" % "jflex" % "1.4.3",
  "org.apache.ant" % "ant" % "1.8.4"
)
