package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, Recovery, RecoveryCompleted, SnapshotSelectionCriteria}

object RecoveryDemo extends App {

  case class Command(contents: String)

  case class Event(id: Int, contents: String)

  class RecoveryActor extends PersistentActor with ActorLogging {

    override def persistenceId: String = "recovery-actor"

    override def receiveCommand: Receive = online(0)

    def online(latestPersistedEventId: Int): Receive = {
      case Command(contents) =>
        persist(Event(latestPersistedEventId, contents)) { event =>
          log.info(s"succesfully persisted $event, recovery is ${if (this.recoveryFinished) "FINISHED" else "not finished yet"}")
          context.become(online(latestPersistedEventId+1))
        }
    }

    override def receiveRecover: Receive = {
      case RecoveryCompleted =>
        // additonal initialization
        log.info("FINISHED RECOVERY")

      case Event(id, contents) => {
//        if (contents.contains("314")) {
//          throw new RuntimeException("I can't take this anymore!")
//        }
        log.info(s"Recovered $contents, recovery is ${if (this.recoveryFinished) "FINISHED" else "not finished yet"}")
        context.become(online(id + 1)) // THIS WILL NOT CHANGE THE HANDLER, receiveRecover will always be used during recovery
        // after recovery the normal handler will be the result of all the stacking of context.become
      }
    }

    override def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      log.error("I failed at recovery")
      super.onRecoveryFailure(cause, event)
    }

//    override def recovery: Recovery = Recovery(toSequenceNr = 100)

//    override def recovery: Recovery = Recovery(fromSnapshot = SnapshotSelectionCriteria.Latest)

//    override def recovery: Recovery = Recovery.none // The recovery will not take place
  }

  val system = ActorSystem("recovery-demo")

  val recoveryActor = system.actorOf(Props[RecoveryActor], "recoveryActor")

  // Stashing of Commands

//  for (i <- 1 to 1000) {
//    recoveryActor ! Command(s"command $i")
//  }

  // ALL COMMANDS DURING RECOVERY ARE STASHED

  // 2 - failure during recovery
  // on recoveryFailure + the actor is stopped - incosistent state

  // 3 - customizing recovery
  // do not persist more events after a customized recovery

  // recovery status or KNOWING when you are done

  // GETTING A SIGNAL WHEN YOUR ARE DONE RECOVERING

  // STATELESS ACTORS
  recoveryActor ! Command(s"special command 1")
  recoveryActor ! Command(s"special command 2")

}
