package par2_event_sourcing

import java.util.Date

import akka.actor.{ActorLogging, ActorSystem, PoisonPill, Props}
import akka.persistence.PersistentActor

object PersistentActors extends App {

  /*
  * we have a business and an accountant which keeps track of our invoices
  * */

  // COMMAND
  case class Invoice(recipient: String, date: Date, amount: Int)

  // EVENT
  case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int)

  case class InvoiceBulk(invoices: List[Invoice])

  case object Shutdown

  class Accountant extends PersistentActor with ActorLogging {

    var latestInvoiceId = 0
    var totalAmount = 0

    def persistenceId: String = "simple-accountant" // best practice : make it unique

    // the normal receive method
    def receiveCommand: Receive = {
      // 1 when you create an EVENT to persist into the store
      // 2 you persist the event, then pass in a callback that will ge triggered once the event is written
      // 3 we update the actors state when the event has persisted
      case Invoice(recipient, date, amount) =>
        log.info(s"Receive invoice for amount: $amount")
        // all other messages send to this actor are stashed

        persist(InvoiceRecorded(latestInvoiceId, recipient, date, amount)) { e =>
          // safe to access mutable state here
          // update state
          latestInvoiceId += 1
          totalAmount += amount

          // correctly identify the sender of the command
          sender ! "persistenceACK"
          log.info(s"Persisted $e as invoice ${e.id}, for total amount $totalAmount")
        }
      case InvoiceBulk(invoices) =>
        val invoiceIds = latestInvoiceId to (latestInvoiceId + invoices.size)
        val events = invoices.zip(invoiceIds) map { pair =>
          val id = pair._2
          val invoice = pair._1
          InvoiceRecorded(id, invoice.recipient, invoice.date, invoice.amount)}
        persistAll(events) { e =>
          latestInvoiceId += 1
          totalAmount += e.amount
          log.info(s"Persisted $e as invoice ${e.id}, for total amount $totalAmount")
        }
        // act like a normal actor
      case Shutdown =>
        context.stop(self)
      case print =>
        log.info(s"Latest invoice id $latestInvoiceId, total amount: $totalAmount")
    }


// handler that will be called on recovery
    def receiveRecover: Receive = {
      // best practice: follow the logic in the persist steps of receiveCommand
      case InvoiceRecorded(id, _, _, amount) =>
        latestInvoiceId = id
        totalAmount += amount
        log.info(s"Recovered invoice #$id for $amount, total amount: $totalAmount")
    }

    /*
    * this method is called if persisting failed
    * the actor will be STOPPED
    *
    * Best practice: start the actor again after a while
    * use backoff supervisor
    * */
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Fail to persist $event because of $cause")
      super.onPersistFailure(cause, event, seqNr)
    }

    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persist rejected for $event because of $cause")
      super.onPersistRejected(cause, event, seqNr)
    }
  }

  val system = ActorSystem("PersistentActors")
  val accountant = system.actorOf(Props[Accountant], "simpleAccountant")

//  for (i <- 1 to 10) {
//    accountant ! Invoice("The sofa company", new Date, i * 1000)
//  }
  val newInvoices = (for (i <- 1 to 10) yield Invoice("The sofa Company", new Date, i * 1000)).toList

  accountant ! InvoiceBulk(newInvoices.toList)
  /*
  * Persistence failures
  *
  * NEVER CALL PERSIST OR PERSISTALL FROM FUTURES
  * */

  accountant ! Shutdown

  // define your own shutdown messages

}
