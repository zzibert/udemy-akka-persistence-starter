package part2_event_sourcing

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor

object PersistAsyncDemo extends App {

  case class Command(contents: String)

  case class Event(contents: String)

  class CriticalStreamProcessor(eventAggregator: ActorRef) extends PersistentActor with ActorLogging {

    override def persistenceId: String = "critical-stream-processor"

    override def receiveCommand: Receive = {
      case Command(contents) =>
        eventAggregator !  s"Proccesing $contents"
        persistAsync(Event(contents)) /* TIME GAP */ { e =>
          eventAggregator ! e
        }

        // some actual computation
        val processedContents = contents + "_processed"

        persistAsync(Event(processedContents)) { e =>
          eventAggregator ! e
        }
    }

    override def receiveRecover: Receive = {
      case message => log.info(s"Recovered: $message")
    }
  }

  object CriticalStreamProcessor {
    def props(eventAggregator: ActorRef) = Props(new CriticalStreamProcessor(eventAggregator))
  }

  class EventAggregator extends Actor with ActorLogging {
    override def receive: Receive = {
      case message => log.info(s"$message")
    }
  }

  val system = ActorSystem("PersistAsyncDemo")

  val eventAggregator = system.actorOf(Props[EventAggregator], "eventAggregator")

  val streamProcessor = system.actorOf(CriticalStreamProcessor.props(eventAggregator), "streamProcessor")

  streamProcessor ! Command("command 1")

  streamProcessor ! Command("command 2")

  /*
  * PersistAsync has the upper hand on performance
  * - high throughput environments
  * - is bad when you the proper ordering of events, when your state depends on it
  * - ordering guarantees
  * */

}
