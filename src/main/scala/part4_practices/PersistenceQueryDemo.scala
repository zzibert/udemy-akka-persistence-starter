package part4_practices

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.journal.{Tagged, WriteEventAdapter}
import akka.persistence.query.{Offset, PersistenceQuery}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.Random

object PersistenceQueryDemo extends App {

  val system = ActorSystem("PersistenceQueryDemo", ConfigFactory.load().getConfig("persistenceQuery"))

  // read jorunal
  val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  // give me all persistence Ids
  val persistenceIds = readJournal.currentPersistenceIds()

  // extran boilerplate
  implicit val materializer = ActorMaterializer()(system)

//  persistenceIds runForeach { persistenceId =>
//    println(s"Found persistenceId: $persistenceId")
//  }

  class SimplePersistentActor extends PersistentActor with ActorLogging {
    override def persistenceId: String = "persistence-query-id-1"

    override def receiveCommand: Receive = {
      case m => persist(m) {_ =>
        log.info(s"Persisted $m")
      }
    }

    override def receiveRecover: Receive = {
      case m => log.info(s"Recovered $m")
    }
  }

//  val simpleActor = system.actorOf(Props[SimplePersistentActor], "simple-persistent-actor")
//
//  import system.dispatcher
//
//  system.scheduler.scheduleOnce(5 seconds) {
//    simpleActor ! "hello persistent actor"
//  }

  // Events by persistence ID
  val events = readJournal.eventsByPersistenceId("persistence-query-id-1", 0, Long.MaxValue)

  events runForeach { event =>
    println(s"Read event: $event")
  }

  // Events by Tags
  val genres = Array("pop", "rock", "hip-hop", "jazz", "metal")

  case class Song(artist: String, title: String, genre: String)

  case class Playlist(songs: List[Song])

  // Event
  case class PlaylistPurchased(id: Int, songs: List[Song])

  class MusicStoreCheckoutActor extends PersistentActor with ActorLogging {
    override def persistenceId: String = "music-store-checkout"

    var latestPlaylistId = 0

    override def receiveCommand: Receive = {
      case Playlist(songs) => {
        persist(PlaylistPurchased(latestPlaylistId, songs)) { _ =>
          log.info(s"User purchased $songs")
          latestPlaylistId += 1
        }
      }
    }

    override def receiveRecover: Receive = {
      case event @ PlaylistPurchased(id, _) => {
        log.info(s"Recovered $event")
        latestPlaylistId = id
      }
    }
  }

  class MusicStoreEventAdapter extends WriteEventAdapter {
    override def manifest(event: Any): String = "musicStore"

    override def toJournal(event: Any): Any = {
      event match {
        case event @ PlaylistPurchased(_, songs) =>
          val genres = songs.map(_.genre).toSet
          Tagged(event, genres)

        case _ => event
      }
    }
  }

  val checkoutActor = system.actorOf(Props[MusicStoreCheckoutActor], "musicStoreActor")

  val r = new Random()

  for (_ <- 1 to 10) {
    val maxSongs = r.nextInt(5)
    val songs = for (i <- 1 to maxSongs) yield {
      val randomGenre = genres(r.nextInt(5))
      Song(s"artist $i", s"title $i", randomGenre)
    }
    checkoutActor ! Playlist(songs.toList)
  }

  val rockPlaylists = readJournal.eventsByTag("rock", Offset.noOffset)

  rockPlaylists runForeach { event =>
    println(s"found a playlist with a rock song $event")
  }
}
