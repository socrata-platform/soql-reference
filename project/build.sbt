resolvers ++= Seq(
  "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.4.1")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.99.5.1")

libraryDependencies ++= Seq(
  "de.jflex" % "jflex" % "1.4.3",
  "org.apache.ant" % "ant" % "1.8.4"
)
