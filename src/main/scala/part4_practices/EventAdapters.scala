package part4_practices

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.{EventSeq, ReadEventAdapter}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

object EventAdapters extends App {

  // store for accoustic guitars
  val ACOUSTIC = "acoustic"
  val ELECTRIC = "electric"

  // data structures
  case class Guitar(id: String, model: String, make: String, guitarType: String = ACOUSTIC)

  // COMMAND
  case class AddGuitar(guitar: Guitar, quantity: Int)


  // EVENT
  case class GuitarAdded(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int)

  case class GuitarAddedV2(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int, guitarType: String = ACOUSTIC)

  class InventoryManager extends PersistentActor with ActorLogging {
    override def persistenceId: String = "guitar-inventory-manager"

    val inventory: mutable.Map[Guitar, Int] = new mutable.HashMap[Guitar, Int]()

    override def receiveCommand: Receive = {
      case AddGuitar(guitar @ Guitar(id, model, make, guitarType), quantity) => {
        persist(GuitarAddedV2(id, model, make, quantity, guitarType)) { _ =>
          updateGuitarQuantity(guitar, quantity)
          log.info(s"Added the $quantity x $guitar to inventory")
        }
      }

      case "print" => {
        log.info(s"Current inventory is $inventory")
      }
    }

    override def receiveRecover: Receive = {
      case event @ GuitarAddedV2(id, model, make, quantity, guitarType) =>
        log.info(s"Recovered new $event")
        val guitar = Guitar(id, model, make, guitarType)
        updateGuitarQuantity(guitar, quantity)
    }

    def updateGuitarQuantity(guitar: Guitar, quantity: Int): Unit = {
      inventory(guitar) = inventory.getOrElse(guitar, 0) + quantity
    }
  }

  class GuitarReadEventAdapter extends ReadEventAdapter {
    // journal -> serializer -> event adapter -> actor
    // bytes -> GuitarAdded -> GuitarAddedV2 -> receiveRecover
    override def fromJournal(event: Any, manifest: String): EventSeq = {
      event match {
        case GuitarAdded(id, model, make, quantity) =>
          EventSeq.single(GuitarAddedV2(id, model, make, quantity, ACOUSTIC))

        case other => EventSeq.single(other)
      }
    }
  }

  // WriteEventAdapter - used for backwards compatibility
  // actor -> write event adapter -> serializer -> journal
  // EventAdapter extend trait

  val system = ActorSystem("EventAdapters", ConfigFactory.load.getConfig("eventAdapters"))

  val inventoryManager = system.actorOf(Props[InventoryManager], "inventoryManager")

//  val guitars = for (i <- 1 to 10) yield Guitar(s"$i", s"Hacker $i", s"RockTheJVM")
//
//  guitars foreach {
//    inventoryManager ! AddGuitar(_, 5)
//  }

  inventoryManager ! "print"


}
