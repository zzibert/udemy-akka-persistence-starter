package part3_stores_and_serialization

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object Cassandra extends App {

  val postgresActorSystem = ActorSystem("cassandraSystem", ConfigFactory.load().getConfig("cassandraDemo"))
  val persistentActor = postgresActorSystem.actorOf(Props[SimplePersistentActor], "simplePersistentActor")

  for (i <- 1 to 10) {
    persistentActor ! s"I love akka $i"
  }

  persistentActor ! "print"

  persistentActor ! "snapshot"

  for (i <- 11 to 20) {
    persistentActor ! s"I love akka $i"
  }

}
