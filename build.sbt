lazy val root = (project in file(".")).settings(
  name := "gumgum-challenge",
  version := "1.0",
  scalaVersion := "2.11.8",
  mainClass in Compile := Some("com.fp.GumGumJoiner"),
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  parallelExecution in Test := true,
  test in assembly := {} // uncomment if you want to skip tests
)

lazy val unprovidedDependencies = Seq(
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

libraryDependencies ++= unprovidedDependencies

val sparkVersion = "2.1.0"

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++= providedDependencies.map(_ % "provided")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case _ => MergeStrategy.first
}