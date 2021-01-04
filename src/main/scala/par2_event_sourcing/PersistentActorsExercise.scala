package par2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

object PersistentActorsExercise extends App {

  /*
  * Persistent actor for a voting station
  * keep:
  * - the citizens who voted
  * - the poll mapping between candidate and the number of received votes so far
  * - the actor must be able to recover its state if its shut down or restarted
  * */
  case class Vote(citizenPID: String, candidate: String)

  case class VoteRecorded(id: Int, citizenPID: String, candidate: String)

  case object LeadingCandidate

  class VotingStation extends PersistentActor with ActorLogging {
    var poll = collection.mutable.Map[String, Int]().withDefaultValue(0)
    var voters: Set[String] = Set()
    var latestVoteId = 0

    def persistenceId: String = "polling-station"

    override def receiveCommand: Receive = {
      case Vote(citizenPID, candidate) => {
        log.info(s"Received vote from $citizenPID for $candidate")
        if (!voters.contains(citizenPID)) {
          persist(VoteRecorded(latestVoteId, citizenPID, candidate)) {e =>
            poll(candidate) += 1
            voters += e.citizenPID
            latestVoteId += 1
            log.info(s"Persisted vote from $citizenPID for $candidate")
          }
        } else {
          log.info(s"citizen $citizenPID already voted")
        }
      }
      case LeadingCandidate =>
        log.info(s"${poll.maxBy(_._2)} has the most votes")
    }

    override def receiveRecover: Receive = {
      case VoteRecorded(_, citizenPID, candidate) =>
        poll(candidate) += 1
        voters += citizenPID
        latestVoteId += 1
        log.info(s"Recovered vote from $citizenPID for $candidate")
    }
  }

  val system = ActorSystem("polling-Station")

  val pollingStation = system.actorOf(Props[VotingStation], "voting-station")

  val votesMap = Map[String, String](
    "Alice" -> "Martin",
    "Bob" -> "Roland",
    "Charlie" -> "Martin",
    "David" -> "Jonaws",
    "Daniel" -> "Martin"
  )

  for ((citizen, candidate) <- votesMap) {
    pollingStation ! Vote(citizen, candidate)
  }

  pollingStation ! LeadingCandidate
}
