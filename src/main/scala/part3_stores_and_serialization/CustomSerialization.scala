package part3_stores_and_serialization

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.serialization.Serializer
import com.typesafe.config.ConfigFactory

// command
case class RegisterUser(email: String, name: String)

// event
case class UserRegistered(id: Int, email: String, name: String)

// serializer
class UserRegistrationSerializer extends Serializer {

  override def identifier: Int = 12345678

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case event @ UserRegistered(id, email, name) =>
      println(s"Serializing $event")
      s"[$id//$email//$name]".getBytes()

    case _ => throw new IllegalArgumentException("Only user registration events supported in this serializer")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val string = new String(bytes)
    val values = string.substring(1, string.length() - 1).split("//")
    val id = values(0).toInt
    val email = values(1)
    val name = values(2)
    val result = UserRegistered(id, email, name)
    println(s"Deserialized $string to $result")
    result
  }

  override def includeManifest: Boolean = false
}

class UserRegistrationActor extends PersistentActor with ActorLogging {

  var currentId = 0

  override def persistenceId: String = "user-registration"

  override def receiveCommand: Receive = {
    case RegisterUser(email, name) =>
      persist(UserRegistered(currentId, email, name)) { e =>
        currentId += 1
        log.info(s"persisted: $e")
      }
  }

  override def receiveRecover: Receive = {
    case event @ UserRegistered(id, _, _) =>
      log.info(s"Recovered $event")
      currentId = id
  }
}

object CustomSerialization extends App {

  // send a command to the actor
  // actor calls persist
  // the serializer serializes the event into bytes
  // the journal writes the bytes

  val system = ActorSystem("CustomSerialization", ConfigFactory.load().getConfig("customSerializerDemo"))
  val userRegistrationActor = system.actorOf(Props[UserRegistrationActor], "userRegistration")

//  for (i <- 1 to 10) {
//    userRegistrationActor ! RegisterUser(s"user_$i@rtjvm.com", s"User $i")
//  }
}
