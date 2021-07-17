package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer}

import scala.collection.mutable

object Snapshots extends App {

  //COMMANDS
  case class ReceivedMessage(contents: String) // Message from your contact

  case class SentMessage(contents: String) // Message to your contact

  //EVENTS
  case class ReceivedMessageRecord(id: Int, contents: String)

  case class SentMessageRecord(id: Int, contents: String)

  class Chat(owner: String, contact: String) extends PersistentActor with ActorLogging {
    val MAX_MESSAGES = 10

    var commandsWithoutCheckpoint = 0
    var currentMessageId = 0
    val lastMessages = new mutable.Queue[(String, String)]()


    override def persistenceId: String = s"$owner-$contact-chat"

    def handleMessage(person: String, contents: String): Unit = {
      if (lastMessages.size >= MAX_MESSAGES) {
        lastMessages.dequeue()
      }
      lastMessages.enqueue((person, contents))
      currentMessageId += 1
    }

    def checkpoint(): Unit = {
      commandsWithoutCheckpoint += 1
      if (commandsWithoutCheckpoint >= MAX_MESSAGES) {
        log.info("Saving Checkpoint")
        saveSnapshot(lastMessages) // asynchronous operation
        commandsWithoutCheckpoint = 0
      }
    }

    override def receiveCommand: Receive = {
      case ReceivedMessage(contents) =>
        persist(ReceivedMessageRecord(currentMessageId, contents)) { e =>
          log.info(s"Received message $contents")
          handleMessage(contact, contents)
          checkpoint()
        }

      case SentMessage(contents) =>
        persist(SentMessageRecord(currentMessageId, contents)) { e =>
          log.info(s"Sent message $contents")
          handleMessage(owner, contents)
          checkpoint()
        }

      case "print" =>
        log.info(s"Most recent messages: $lastMessages \n")

      // snapshot related messages
      case SaveSnapshotSuccess(metadata) =>
        log.info("Saving snapshot succeeded", metadata)

      case SaveSnapshotFailure(metadata, reason) =>
        log.warning(s"Saving snapshot $metadata failed because of $reason")
    }

    override def receiveRecover: Receive = {
      case ReceivedMessageRecord(id, contents) =>
        log.info(s"Recovered received message id $id: $contents")
        handleMessage(contact, contents)

      case SentMessageRecord(id, contents) =>
        log.info(s"Recovered sent message id $id: $contents")
        handleMessage(owner, contents)

      case SnapshotOffer(metadata, contents) =>
        log.info(s"Recovered Snapshot: $metadata")
        contents.asInstanceOf[mutable.Queue[(String, String)]].foreach(lastMessages.enqueue(_))
    }
  }

  object Chat {
    def props(owner: String, contact: String) = Props(new Chat(owner, contact))
  }

  val system = ActorSystem("Snapshots-demo")

  val chat = system.actorOf(Chat.props("daniel123", "martin345"))

//  for (i <- 1 to 100000) {
//    chat ! ReceivedMessage(s"final countdown $i")
//    chat ! SentMessage(s"trollololololololo $i")
//  }

  chat ! "print"

  // SNAPSHOTS


  // optional , best practice : handle SaveSnapshotSuccess and SaveSnapshotFailure in ReceiveCommand
  // profit from the extra speed

}
