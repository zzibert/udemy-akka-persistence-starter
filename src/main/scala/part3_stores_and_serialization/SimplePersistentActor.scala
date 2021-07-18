package part3_stores_and_serialization

import akka.actor.ActorLogging
import akka.persistence.{PersistentActor, RecoveryCompleted, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer}

private class SimplePersistentActor extends PersistentActor with ActorLogging {
    override def persistenceId: String = "simple-persistent-actor"

    // mutable state
    var nMessages = 0

    override def receiveCommand: Receive = {
      case "print" =>
        log.info(s"I have persisted $nMessages so far")

      case "snapshot" =>
        saveSnapshot(nMessages)

      case SaveSnapshotSuccess(metadata) =>
        log.info(s"SAve snapshot was succesfull: $metadata")

      case SaveSnapshotFailure(_, cause) =>
        log.warning(s"Save snapshot failed: $cause")

      case message =>
        persist(message) { e =>
          log.info(s"Persisting message $e")
          nMessages += 1
        }
    }

    override def receiveRecover: Receive = {
      case RecoveryCompleted =>
        log.info("Recovery Done")

      case SnapshotOffer(_, payload: Int) =>
        log.info(s"Recovered snapshot: $payload")
        nMessages = payload

      case message =>
        log.info(s"Recovered $message")
        nMessages += 1

    }
  }