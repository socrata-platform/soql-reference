resolvers ++= Seq(
  "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release",
  "velvia maven" at "http://dl.bintray.com/velvia/maven"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.3.2")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.99.5.1")

libraryDependencies ++= Seq(
  "de.jflex" % "jflex" % "1.4.3",
  "org.apache.ant" % "ant" % "1.8.4"
)
