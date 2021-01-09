package par2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer}

import scala.collection.mutable

object Snapshots extends App {

  // comamands
  case class ReceivedMessage(contents: String) // message from your contact
  case class SentMessage(contents: String) // message TO your contact

  // events
  case class ReceivedMessageRecord(id: Int, contents: String)
  case class SentMessageRecord(id: Int, contents: String)

  object Chat {
    def props(owner: String, contact: String) = Props(new Chat(owner, contact))
  }

  class Chat(owner: String, contact: String) extends PersistentActor with ActorLogging {

    var commandsWithoutCheckpoints = 0
    var currentMessageId = 0
    val MAX_MESSAGES = 10
    val lastMessages = new mutable.Queue[(String, String)]()

    def updateLastMessages(person: String, contents: String): Unit = {
      if (lastMessages.size >= MAX_MESSAGES) {
        lastMessages.dequeue()
      }
      lastMessages.enqueue((person, contents))
    }

    def maybeCheckpoint(): Unit = {
      commandsWithoutCheckpoints += 1
      if (commandsWithoutCheckpoints >= MAX_MESSAGES) {
        log.info(s"saving checkpoint")
        saveSnapshot(lastMessages) // asynchrounous operation
        commandsWithoutCheckpoints = 0
      }
    }

    override def persistenceId: String = s"$owner-$contact-chat"

    override def receiveCommand: Receive = {
      case ReceivedMessage(contents) =>
        persist(ReceivedMessageRecord(currentMessageId, contents)) { e =>
          log.info(s"received message $contents")
          updateLastMessages(contact, contents)
          currentMessageId += 1
          maybeCheckpoint()
        }
      case SentMessage(contents) =>
        persist(SentMessageRecord(currentMessageId, contents)) { e =>
          log.info(s"sent message $contents")
          updateLastMessages(owner, contents)
          currentMessageId += 1
          maybeCheckpoint()
        }
      case "print" =>
        log.info(s"last received message $lastMessages")
      // snapshot related messages
      case SaveSnapshotSuccess(metadata) =>
        log.info(s"saving snapshot succeeded $metadata")
      case SaveSnapshotFailure(metada, reason) => log.info(s"saving snapshot $metada failed because $reason")
    }

    override def receiveRecover: Receive = {
      case ReceivedMessageRecord(id, contents) => {
        log.info(s"restored received message $id: $contents")
        updateLastMessages(contact, contents)
        currentMessageId = id
      }
      case SentMessageRecord(id, contents) => {
        log.info(s"restored sent message $id: $contents")
        updateLastMessages(owner, contents)
        currentMessageId = id
      }
      case SnapshotOffer(metadata, contents) =>
        log.info(s"recovered snapshot $metadata")
        contents.asInstanceOf[mutable.Queue[(String, String)]].foreach {
          lastMessages.enqueue(_)
        }
    }
  }

  val system = ActorSystem("SnapshotsDemo")
  val chat = system.actorOf(Chat.props("daniel123", "martin123"))

//  for (i <- 1 to 100000) {
//    chat ! ReceivedMessage(s"Akka blows $i")
//    chat ! SentMessage(s"I know  $i")
//  }

  chat ! "print"
}
