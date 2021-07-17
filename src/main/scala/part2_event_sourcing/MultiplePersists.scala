package part2_event_sourcing

import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor

object MultiplePersists extends App {

  // Diligent accoutant, with every invoice , will persist TWO events
  // - a tax record for the fiscal authority
  // - an invoce record for personal logs or some auditing authority

  // COMMAND
  case class Invoice(recipient: String, date: Date, amount: Int)

  // EVENTS
  case class TaxRecord(taxId: String, recordId: Int, date: Date, totalAmount: Int)

  case class InvoiceRecord(invoiceRecordId: Int, recipient: String, date: Date, amount: Int)

  class TaxAuthority extends Actor with ActorLogging {
    override def receive: Receive = {
      case message => log.info(s"Received message $message")
    }
  }

  class DiligentAccountant(taxId: String, taxAuthority: ActorRef) extends PersistentActor with ActorLogging {

    var latestTaxRecordId = 0
    var latestInvoiceRecordId = 0

    override def persistenceId: String = "diligent-accountant"

    override def receiveCommand: Receive = {
      case Invoice(recipient, date, amount) =>
        // journal ! TaxRecord
        persist(TaxRecord(taxId, latestTaxRecordId, date, amount / 3)) { record =>
          taxAuthority ! record
          latestTaxRecordId += 1

          persist("I hereby declare this tax record to be true and complete") { decleration =>
            taxAuthority ! decleration
          }
        }
        // journal ! InvoiceRecord
        persist(InvoiceRecord(latestTaxRecordId, recipient, date, amount)) { invoiceRecord =>
          taxAuthority ! invoiceRecord
          latestInvoiceRecordId += 1

          persist("I hereby declare this invoice record to be true and complete") { decleration =>
            taxAuthority ! decleration
          }
        }
    }

    override def receiveRecover: Receive = {
      case event => log.info(s"Recovered $event")
    }
  }

  object DiligentAccountant {
    def props(taxId: String, taxAuthority: ActorRef) = Props(new DiligentAccountant(taxId, taxAuthority))
  }

  val system = ActorSystem("multiple-persists-demo")

  val taxAuthority = system.actorOf(Props[TaxAuthority], "the_fiscal_authority")

  val accountant = system.actorOf(DiligentAccountant.props("UK2435233423", taxAuthority))

  accountant ! Invoice("The Sofa company", new Date, 2000)

  accountant ! Invoice("The Super car company", new Date, 4000)

  // messge ordering (TaxRecord -> Invoice record) is guaranteed
  // persistence is also based on message passing, journals are implemented in temrns

}
