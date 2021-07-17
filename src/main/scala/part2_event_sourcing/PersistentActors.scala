package part2_event_sourcing

import java.util.Date

import akka.actor.{ActorLogging, ActorSystem, PoisonPill, Props}
import akka.persistence.PersistentActor

object PersistentActors extends App {

  /*
  * Scenario: we have a business and an accountant which keeps track of our invoices
  */

  // COMMANDS
  case class Invoice(recipient: String, date: Date, amount: Int)

  case class InvoiceBulk(invoices: List[Invoice])

  // SPECIAL MESSAGES
  case object Shutdown

  // EVENTS
  case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int)

  class Accountant extends PersistentActor with ActorLogging {

    var latestInvoiceId = 0
    var totalAmount = 0

    override def persistenceId: String = "simple-accountant" // best practice: make it unique

    // the normal receive method
    override def receiveCommand: Receive = {
      case Shutdown =>
        context.stop(self)

      case InvoiceBulk(invoices) =>
        // create events (plural)
        // persist all the events
        // update the actor state , when each event is persisted
        val invoiceIds = latestInvoiceId to (latestInvoiceId + invoices.size)
        val events = invoices.zip(invoiceIds) map { pair =>
          val id = pair._2
          val invoice = pair._1

          InvoiceRecorded(id, invoice.recipient, invoice.date, invoice.amount)
        }
        persistAll(events) { e =>
          // persist each event in sequence, triggers callback for each event persisted
          latestInvoiceId += 1
          totalAmount += e.amount
          log.info(s"Received SINGLE invoice for the amount: ${e.amount}")
        }
      case Invoice(recipient, date, amount) =>
        // when you receive a command
        // you create an event to persist into the store
        // you persist the event, then pass in a callback that will get triggered, once the event is written
        // we update the actor state when the event has persisted
        log.info("Receive invoice for the amount: $amount")
        persist(InvoiceRecorded(latestInvoiceId, recipient, date, amount)) /* time gap: all other messages send to this actor are stashed */
        { e =>
          // safe to access mutable state
          latestInvoiceId += 1
          totalAmount += amount

          // correctly identify the sender of the COMMAND
//          sender() ! "PersistenceACK"
          log.info(s"Persisted $e as invoice # ${e.id} for total amount $totalAmount")
        }
        // act like a normal actor
      case "print" =>
        log.info(s"Latest invoice $latestInvoiceId, total amount: $totalAmount")

    }

    // the handler being called on recovery
    override def receiveRecover: Receive = {
      /*
      * Follow the log in the persist steps of receiveCommand
      */
      case InvoiceRecorded(id, _, _, amount) =>
        latestInvoiceId = id
        totalAmount += amount
        log.info(s"Recovered invoice #${id} for amount $amount, total amount $totalAmount")
    }
    // this method is called if persisting failed.
    // the actor will be stopped

    // Best practice: Start the actor again after a while, use backoff supervisor
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Fail to persist $event because of $cause")
      super.onPersistFailure(cause, event, seqNr)
    }

    // called if JOURNAL fails to persist the event
    // the actor is resumed
    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persist rejected for $event because of $cause")
        super.onPersistRejected(cause, event, seqNr)
    }
  }

  val system = ActorSystem("PersistentActors")
  val accountant = system.actorOf(Props[Accountant], "simpleAccountant")


//  accountant ! InvoiceBulk(newInvoices.toList)

  for (i <- 1 to 10) {
    accountant ! Invoice("The Sofa company", new Date, i * 1000)
  }

  /*
  * Persistence failures
  * */

  // Persisting multiple events
  val newInvoices = for(i <- 1 to 5) yield Invoice("The awesome chairs", new Date, 1000 * i)

  // never ever call persist or persistAll from futures !!!

  /*
  * Shutdown of persistent actors
  * */

//  accountant ! PoisonPill
  // define your own shutdown messages

  accountant ! Shutdown
}
