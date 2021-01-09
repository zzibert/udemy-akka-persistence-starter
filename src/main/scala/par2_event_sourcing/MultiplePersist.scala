package par2_event_sourcing

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor

object MultiplePersist extends App {

  /*
  * Diligent Accpimt: with every invoice , will persist two events
  * - a tax record for the fiscal authority
  * - an invoice record for personal logs or some auditing authority
  * */

  case class Invoice(recipient: String, date: Date, amount: Int)

  case class TaxRecord(taxId: String, recordId: Int, date: Date, totalAmount: Int)

  case class InvoiceRecord(invoiceRecordId: Int, recipient: String, date: Date, amount: Int)

  object DiligentAccountant {
    def props(taxId: String, taxAuthority: ActorRef) = Props(new DiligentAccountant(taxId, taxAuthority))
  }

  class DiligentAccountant(taxId: String, taxAuthority: ActorRef) extends PersistentActor with ActorLogging {

    var latestTaxRecordId = 0
    var latestInvoiceRecordId = 0

    override def persistenceId: String = "diligent-accountant"

    override def receiveCommand: Receive = {
      case Invoice(recipient, date, amount) => {
        // journal ! TaxRecord
        persist(TaxRecord(taxId, latestTaxRecordId, date, amount / 3)) { record =>
          taxAuthority ! record
          latestTaxRecordId += 1

          persist("I hereby declare this tax record to be true and complete") { decleration =>
            taxAuthority ! decleration
          }
        }
        // jounral ! InvoiceRecord
        persist(InvoiceRecord(latestInvoiceRecordId, recipient, date, amount)) { invoiceRecord =>
          taxAuthority ! invoiceRecord
          latestInvoiceRecordId += 1

          persist("I hereby declare this invoice record to be true and complete") { decleration =>
            taxAuthority ! decleration
          }
        }
      }
    }

    override def receiveRecover: Receive = {
      case event => log.info("Recovered $event")
    }
  }

  class TaxAuthority extends Actor with ActorLogging {
    override def receive: Receive = {
      case message => log.info(s"Received: $message")
    }
  }

  val system = ActorSystem("MultiplePersistsDemo")

  val taxAuthority = system.actorOf(Props[TaxAuthority], "HMRC")

  val accountant = system.actorOf(DiligentAccountant.props("UK234234", taxAuthority))

  accountant ! Invoice("the sofa Company", new Date, 2000)

  accountant ! Invoice("the car comapny", new Date, 3000)

  // the message ordering is guaranteed

  // Persistence is also based on message passing

  // nested persisting



}
