package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import scala.collection.mutable

object PersistentActorsExercise extends App {

  // Persistent actor for a voting station
  // keep
  // - the citizen who voted
  // - the poll mapping between candidate and the number of received votes
  // the actor must be able to recover its state, if its shutdown or restarted

  // COMMAND
  case class Vote(citizenPID: String, candidate: String)

  case object Winner

  case object State

  // EVENTS
  case class VoteRecorded(id: Int, citizenPID: String, candidate: String)

  class PollingStation extends PersistentActor with ActorLogging {

    var latestVoteId = 0
    val citizensWhoVoted = mutable.Set[String]()
    val poll = mutable.Map[String, Int]()

    override def persistenceId: String = "polling-station"

    override def receiveCommand: Receive = {
      case Vote(citizenPID, candidate) =>
        if (citizensWhoVoted.contains(citizenPID)) {
          log.info(s"Citizen $citizenPID already voted")
        } else {
          persist(VoteRecorded(latestVoteId, citizenPID, candidate)) {e =>
            citizensWhoVoted += citizenPID
            poll(candidate) = poll.getOrElse(candidate, 0) + 1
            latestVoteId += 1
            log.info(s"Citizen $citizenPID voted for $candidate")
          }
        }

      case Winner =>
        val candidate = poll.maxBy(pair => pair._2)
        log.info(s"The winner is $candidate")

      case State =>
        log.info(s"The state of the poll is ${poll}")
    }

    override def receiveRecover: Receive = {
      case VoteRecorded(id, citizenPID, candidate) =>
        citizensWhoVoted += citizenPID
        poll(candidate) = poll.getOrElse(candidate, 0) + 1
        latestVoteId += 1
        log.info(s"Recovered vote #${id}, Citizen $citizenPID voted for $candidate")
    }
  }

  val system = ActorSystem("PersistentActors")
  val pollingStation = system.actorOf(Props[PollingStation], "polling-station")

//  pollingStation ! Vote("fahri", "biden")
//  pollingStation ! Vote("adam", "biden")
//  pollingStation ! Vote("paul", "trump")
//  pollingStation ! Vote("fahri", "trump")

  val votesMap = Map[String, String](
    "fahri" -> "biden",
    "adam" -> "biden",
    "paul" -> "trump",
    "fahri" -> "trump"
  )

  votesMap.foreach { pair =>
    pollingStation ! Vote(pair._1, pair._2)
  }

  pollingStation ! Winner

  pollingStation ! State
}
