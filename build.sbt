name := "Gestion_contrat"
version := "1.0"
scalaVersion := "2.12.15"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "com.beust" % "jcommander" % "1.48"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.15.0"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.19" % Test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.3.1_2.0.1" % Test
Test / fork := true
Test / javaOptions += "-Dscala.use.classpath.file=true"