inThisBuild(
  List(
    organization := "com.kubukoz",
    homepage := Some(url("https://github.com/kubukoz/kafka-demo")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "kubukoz",
        "Jakub Kozłowski",
        "kubukoz@gmail.com",
        url("https://kubukoz.com")
      )
    )
  )
)

val compilerPlugins = List(
  compilerPlugin("org.typelevel" % "kind-projector"      % "0.11.0" cross CrossVersion.full),
  compilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1")
)

val commonSettings = Seq(
  scalaVersion := "2.13.1",
  scalacOptions ~= (_.filterNot(_ == "-Xfatal-warnings") ++ Seq(
    "-Ymacro-annotations",
    "-Yimports:" ++ List(
      "scala",
      "scala.Predef",
      "cats",
      "cats.implicits",
      "cats.effect",
      "cats.effect.implicits",
      "cats.effect.concurrent"
    ).mkString(",")
  )),
  fork in Test := true,
  updateOptions := updateOptions.value.withGigahorse(false),
  resolvers += "confluent" at "https://packages.confluent.io/maven/",
  libraryDependencies ++= Seq(
    "org.http4s"     %% "http4s-blaze-server"  % "0.21.0-M5",
    "org.http4s"     %% "http4s-dsl"           % "0.21.0-M5",
    "org.http4s"     %% "http4s-circe"         % "0.21.0-M5",
    "dev.profunktor" %% "console4cats"         % "0.8.0",
    "ch.qos.logback" % "logback-classic"       % "1.2.3",
    "org.typelevel"  %% "cats-tagless-macros"  % "0.10",
    "io.circe"       %% "circe-generic-extras" % "0.12.2",
    "com.olegpy"     %% "meow-mtl-core"        % "0.4.0",
    "com.olegpy"     %% "meow-mtl-effects"     % "0.4.0",
    "com.ovoenergy"  %% "fs2-kafka-vulcan"     % "0.20.2",
    "com.ovoenergy"  %% "vulcan-generic"       % "0.3.1",
    "org.tpolecat"   %% "skunk-core"           % "0.0.7",
    "org.tpolecat"   %% "skunk-circe"          % "0.0.7",
    "io.estatico"    %% "newtype"              % "0.4.3",
    "org.scalatest"  %% "scalatest"            % "3.1.0" % Test
  ) ++ compilerPlugins
)

def app(name: String) =
  Project(name, file(s"applications/$name")).settings(commonSettings).enablePlugins(JavaAppPackaging)

val events = project.in(file("applications/events")).settings(commonSettings)

val stock   = app("stock").dependsOn(events)
val reports = app("reports").dependsOn(events)

val root =
  project
    .in(file("."))
    .settings(name := "kafka-demo", commonSettings)
    .settings(skip in publish := true)
    .aggregate(stock, reports)
