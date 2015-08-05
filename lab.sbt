name := "ProjectLab"

version := "1.0"

scalaVersion := "2.10.4"

assemblyMergeStrategy in assembly := { 
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.0" % "provided"
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

