import akka.actor._
import akka.routing.RoundRobinRouter
import scala.util._
import scala.collection.mutable.ArrayBuffer
import java.security.MessageDigest
import com.typesafe.config.ConfigFactory

sealed trait BitcoinMinerMessage
case class FindBitcoins(startingStrLen: Int, endingStrLen: Int, numZeros: Int) extends BitcoinMinerMessage
case class Work(strLength: Int, nrOfWkrItrs: Int, numZeros: Int) extends BitcoinMinerMessage
case class Result(bitcoins: ArrayBuffer[String]) extends BitcoinMinerMessage
case class RemoteResult(bitcoins: ArrayBuffer[String]) extends BitcoinMinerMessage
case class SendRange() extends BitcoinMinerMessage 
case class GetRange(startingStrLen: Int, endingStrLen: Int, numZeros: Int) extends BitcoinMinerMessage
case class GetWorkFromSuperMaster() extends BitcoinMinerMessage
case class RemoteCompleted() extends BitcoinMinerMessage

class Worker extends Actor {
  //Random generator
  val random = new Random
				 
  //Generate a random string of length n from the given characters
  def randomString(characters: String)(n: Int): String = 
  Stream.continually(random.nextInt(characters.size)).map(characters).take(n).mkString
				 
  //Generate a random string of length n (using printable characters from ASCII 33 to ASCII 126)
  def randomCharactersString(n: Int) = 
  randomString("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")(n)
	
  //Calculate the hash for the randomly generated input string
  def calcHash(inputString:String):String = 
  {
    val result: String = MessageDigest.getInstance("SHA-256").digest(inputString.getBytes).map("%02x".format(_)).mkString
    return result
  }
	
  def receive = {
    case Work(strLength, nrOfWkrItrs, numZeros) =>
      var bitcoins = ArrayBuffer[String]()
      for (i <- 0 until nrOfWkrItrs)
      {
        //Generate random string of length strLength
		val randomString:String = randomCharactersString(strLength)
		val inputString = "vchelamkuri" + randomString + i
	    
		//Calculate the SHA-256 hash
		val hash = calcHash(inputString)	  
	    
		//Check if hash satisfies the input bitcoin requirement
		val searchString =  "0" * numZeros
		val startsWithZeros:Boolean = hash.startsWith(searchString)
		if(startsWithZeros)
		{
		  //Add to bitcoins ArrayBuffer
	      bitcoins += inputString + "\t" + hash
		}
      }
      sender ! Result(bitcoins)
  }
}

class Master(nrOfWorkers: Int, nrOfMessages: Int, nrOfWkrItrs: Int, input: String) extends Actor {
  var nrOfRemoteMsgs: Int = _
  var nrOfRemoteResults: Int = _
  var nrOfLocalResults: Int = _
  var localBitcoins = ArrayBuffer[String]()
  var remoteStartRanges: ArrayBuffer[Int] = ArrayBuffer(16, 5, 11)
  var remoteEndRanges: ArrayBuffer[Int] = ArrayBuffer(20, 10, 15)
  val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")
  var superMaster: ActorRef = _
  if(input.contains('.'))
  {
    val connectionString = "akka.tcp://ServerMinerSystem@" + input + ":4455/user/master"
    superMaster = context.actorFor(connectionString)
  }

  def receive = {
    case FindBitcoins(startingStrLen, endingStrLen, numZeros) =>
      //Send string lengths between a start and end range
      var strLength: Int = startingStrLen
      for (i <- 0 until nrOfMessages)
      {
        workerRouter ! Work(strLength, nrOfWkrItrs, numZeros)
        strLength += 1
        
        //Reset string length if it exceeds endingStrLen
        if(strLength == endingStrLen)
          strLength = startingStrLen
      }

    case Result(bitcoins) =>
      nrOfLocalResults += 1
      
      if(input.contains('.'))
      {
        localBitcoins ++= bitcoins
        if(localBitcoins.size > 100)
        {
          //If size of local bitcoins buffer is > 100, then send them to server for
          //printing and clear the local bitcoins buffer for holding more results
          var bitcoinsToBeSent = ArrayBuffer[String]()
          bitcoinsToBeSent = localBitcoins.distinct
          superMaster ! RemoteResult(bitcoinsToBeSent)
          localBitcoins.clear()
        }
        if (nrOfLocalResults == nrOfMessages)
        {
          //Send the remaining bitcoins (that are < 100) to server for printing, clear the
          //the local bitcoins buffer and indicate to server that the work is completed
          var bitcoinsToBeSent = ArrayBuffer[String]()
          bitcoinsToBeSent = localBitcoins.distinct
          superMaster ! RemoteResult(bitcoinsToBeSent)
          localBitcoins.clear()
          superMaster ! RemoteCompleted()
          
          //Shutdown client
          println("Shutting down client...")
          context.system.shutdown()
        }
      }
      else 
      {
          //If acting as server, print the received bitcoins
    	  for (bitcoin <- bitcoins) 
            println("%s".format(bitcoin))
            
          if (nrOfLocalResults == nrOfMessages && nrOfRemoteResults == nrOfRemoteMsgs)
          {
            //Shutdown server
            println("Shutting down server...")
            context.system.shutdown()
          }
      }
      
    case RemoteResult(bitcoins) =>
      //Print the results received from remote workers
      for (bitcoin <- bitcoins) 
        println("%s".format(bitcoin))
    
    case RemoteCompleted() =>
      nrOfRemoteResults += 1
      if (nrOfLocalResults == nrOfMessages && nrOfRemoteResults == nrOfRemoteMsgs)
      {
        //Shutdown server
        println("Shutting down server...")
        context.system.shutdown()
      }
      
    case SendRange() =>
      println("Sending work...")
      nrOfRemoteMsgs += 1
      var workIndex = (nrOfRemoteMsgs % 3)
      sender ! GetRange(remoteStartRanges(workIndex), remoteEndRanges(workIndex), input.toInt)
      
    case GetRange(startingStrLen, endingStrLen, numZeros) =>
      println("Got work...")
      self ! FindBitcoins(startingStrLen, endingStrLen, numZeros)
      
    case GetWorkFromSuperMaster() =>
      println("Getting work...")
      superMaster ! SendRange()
  }
}

object project1 extends App {
  val input = args(0)
  findBitcoins(nrOfWorkers = 12, nrOfWkrItrs = 100000, nrOfMessages = 300, input)
  def findBitcoins(nrOfWorkers: Int, nrOfWkrItrs: Int, nrOfMessages: Int, input: String) {
    //Act as client
    if(input.contains('.'))
    {
      val system = ActorSystem("ClientMinerSystem")
      val master = system.actorOf(Props(new Master(nrOfWorkers, nrOfMessages, nrOfWkrItrs, input)), name = "master")
      master ! GetWorkFromSuperMaster()
    }
    //Act as server
    else
    {
      val system = ActorSystem("ServerMinerSystem")
      val master = system.actorOf(Props(new Master(nrOfWorkers, nrOfMessages, nrOfWkrItrs, input)), name = "master")
      master ! FindBitcoins(10, 20, input.toInt)
    }
  }
}