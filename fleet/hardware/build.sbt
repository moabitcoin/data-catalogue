name := "schemas"

scalaVersion := "2.12.8"


libraryDependencies ++= Seq(

  "org.apache.avro" % "avro" % "1.8.2" % Test,
  "org.scalatest" %% "scalatest" % "3.0.7" % Test,
  "org.mockito" % "mockito-core" % "2.27.0" % Test,
  "org.hamcrest" % "hamcrest-library" % "2.1" % Test,
  "com.github.pathikrit" %% "better-files" % "3.8.0" % Test

)