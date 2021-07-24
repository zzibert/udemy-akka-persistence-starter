package part4_practices

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.{EventAdapter, EventSeq}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

object DetachingModels extends App {
  import DomainModel._

  class CouponManager extends PersistentActor with ActorLogging {

    val coupons: mutable.Map[String, User] = new mutable.HashMap[String, User]()

    override def persistenceId: String = "coupon-manager"

    override def receiveCommand: Receive = {
      case ApplyCoupon(coupon, user) =>
        if (!coupons.contains(coupon.code)) {
          persist(CouponApplied(coupon.code, user)) { e =>
            log.info(s"Persisted event $e")
            coupons.put(coupon.code, user)
          }
        }
    }

    override def receiveRecover: Receive = {
      case event @ CouponApplied(code, user) =>
        log.info(s"Recovered $event")
        coupons.put(code, user)
    }
  }

  val system = ActorSystem("DetachingModels", ConfigFactory.load().getConfig("detachingModels"))

  val couponManager = system.actorOf(Props[CouponManager], "coupon-manager")

  val users = for (i <- 1 to 5) yield User(s"user $i", s"email-$i-com", s"name $i")

  val coupons = for (c <- 1 to 5) yield Coupon(s"code-$c-code", c * 10)

//  users.zip(coupons) foreach { pair =>
//    couponManager ! ApplyCoupon(pair._2, pair._1)
//  }

}

object DomainModel {

  case class User(id: String, email: String, name: String)
  case class Coupon(code: String, promotionAmount: Int)

  // COMMAND
  case class ApplyCoupon(coupon: Coupon, user: User)

  // EVENT

  case class CouponApplied(code: String, user: User)
}

object DataModel {
  case class WrittenCouponApplied(code: String, userId: String, userEmail: String)
  case class WrittenCouponAppliedV2(code: String, userId: String, userEmail: String, userName: String)
}

class ModelAdapter extends EventAdapter {
  import DomainModel._
  import DataModel._

  override def manifest(event: Any): String = "CMA"

  // journal -> serializer -> fromJournal -> Actor
  override def fromJournal(event: Any, manifest: String): EventSeq = event match {
    case event @ WrittenCouponApplied(code, userId, userEmail) =>
      println(s"Converting $event to domain model")
      EventSeq.single(CouponApplied(code, User(userId, userEmail, "torlololo")))

    case event @ WrittenCouponAppliedV2(code, userId, userEmail, userName) =>
      println(s"Converting $event to domain model")
      EventSeq.single(CouponApplied(code, User(userId, userEmail, userName)))

    case other => EventSeq.single(other)
  }

  // actor -> toJournal -> serializer -> journal
  override def toJournal(event: Any): Any = event match {
    case event @ CouponApplied(code, user) =>
      println(s"Converting $event to data model")
      WrittenCouponAppliedV2(code, user.id, user.email, user.name)
  }
}
