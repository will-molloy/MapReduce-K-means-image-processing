name := "spark-scala-kmeans"

version := "0.1"

scalaVersion := "2.11.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
// https://mvnrepository.com/artifact/io.humble/humble-video-all
libraryDependencies += "io.humble" % "humble-video-all" % "0.2.1"
// https://mvnrepository.com/artifact/org.bytedeco/javacv
libraryDependencies += "org.bytedeco" % "javacv" % "1.4.1"
