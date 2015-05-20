import akka.actor._
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.util._
import scala.collection._

sealed trait GossipMessage
case class Converge() extends GossipMessage
case class Rumor() extends GossipMessage
case class Remind() extends GossipMessage
case class PushConverge(swRatio:Double) extends GossipMessage
case class PushRumor(sum:Double,weight:Double) extends GossipMessage
case class PushRemind() extends GossipMessage
  
object project2bonus
{
  var master:ActorRef = null
  val system = ActorSystem("GossipSystem")
  
  def main(args: Array[String])
  {
    var numNodes = args(0).toInt
    val topology = args(1)
    val algorithm = args(2)
    var failureNodeCount = 0
    if(args.length == 4)
      failureNodeCount = args(3).toInt
	  	
    //For 2D based topologies you can round up until you get a square
    if(topology == "2D" || topology == "imp2D") 
    {
      var sqroot = (Math.sqrt(numNodes))  
      numNodes = sqroot.intValue*sqroot.intValue
    }
	  	
    master = system.actorOf(Props(new Master(numNodes, topology, algorithm, failureNodeCount)), name = "master")
  }
  
  class Master(numNodes:Int, topology:String, algorithm:String, failureNodeCount:Int) extends Actor
  {
    //Indicates number of actors (a) who have received the message at least once or, (b) whose s/w ratio has converged
    var convergenceCount: Int = 0
    //An array of all the original actors in the system
    val originalActors:ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]()
    //An array indicating which actors are still alive
    val isAlive = new Array[Boolean](numNodes);
    for(i:Int <- 0 to numNodes-1)
    {	
      isAlive(i) = true;
    }

    //Randomly mark some nodes as dead
    val h = new collection.mutable.HashMap[Int,Int]
    var j = 0
    while(j <= (failureNodeCount-1))
    {
      var random = Random.nextInt(numNodes)
      
      //Make sure that you are not marking the same node as dead twice
      if(!h.contains(random))
      {
        if((random == 0) || (random == (numNodes-1)) )
	      println("Node:"+random+" is one of the failure nodes.")
    	h(j) = random
    	isAlive(random) = false;
    	j = j+1
      }
    }

    //Initial actor randomly selected by the main process (i.e. selected by Master)
    //Keep selecting an initial actor until you get an actor that is not dead.
    var intialActor:Int = Random.nextInt(numNodes)
    while(!isAlive(intialActor))
    {
      intialActor = Random.nextInt(numNodes)
    }

    //Holds the start time of the protocol
    var startTime:Long = 0;
    
    algorithm match
    {
      case "gossip" =>
        var i:Int = 0
        for(i<- 0 to (numNodes-1))
        {
          originalActors += context.actorOf(Props(new GossipActor(i.toInt, topology, numNodes, originalActors, isAlive)), name = i+toString)
        }
        //Start the algorithm/protocol
        startTime = System.currentTimeMillis()
        //Select a random actor to start with.
        originalActors(intialActor) ! Rumor
        
      case "push-sum" =>
        var i:Int = 0
        for(i<- 0 to (numNodes-1))
        {
          originalActors += context.actorOf(Props(new GossipActor(i.toInt, topology, numNodes, originalActors, isAlive)), name = i+toString)
        }
        //Start the algorithm/protocol
        startTime = System.currentTimeMillis()
        //Select a random actor to start with. Initially, pass sum=0 and weight=0 (so that the Master actor does not influence the push-sum in any way)
        originalActors(intialActor) ! PushRumor(0, 0)
    }
    
    def receive =    
    {
      case Converge =>   
        convergenceCount = convergenceCount + 1  
        if(convergenceCount == (numNodes-failureNodeCount))
        {
          println("Gossip convergence completed.")
          var endTime:Long = System.currentTimeMillis()
          println("Time for convergence: " + (endTime - startTime) + "ms")
          
          println("Shutting down the system.")
          context.system.shutdown
        }
        
        case PushConverge(swRatio) => 
          convergenceCount = convergenceCount + 1
          //If one node has converged, then all nodes have converged.
          if(convergenceCount == 1)
          {
            println("Push-sum convergence completed.")
            var endTime:Long = System.currentTimeMillis()
            println("Time for convergence: " + (endTime - startTime) + "ms")
            println("Convergence Ratio: "+swRatio)
          
            println("Shutting down the system.")
            context.system.shutdown
          }
    }
  }
  
  class GossipActor(actorId:Int, topology:String, numNodes:Int, originalActors:ArrayBuffer[ActorRef], isAlive:Array[Boolean]) extends Actor
  {
    var noOfMsgsReceived: Int = 0
    var roundCount: Int = 0
    var sValue:Double = actorId+1
    var wValue: Double = 1
    var previousSWRatio: Double = 0
    
    def receive =
    {
      //Gossip - Start
      
      case Rumor =>
      	noOfMsgsReceived = noOfMsgsReceived + 1
      	if(noOfMsgsReceived == 1)
      	{
      	  master ! Converge
      	  
      	  //Start transmitting message to random neighbors (in a loop with some delay)
      	  self ! Remind
      	}
      	
      	if(noOfMsgsReceived == 10)
      	{
      	  //Update the isAlive array
          isAlive(actorId) = false;
        
      	  //Self-destruct
      	  context.stop(self)
      	}
      	
      case Remind =>
	    topology match
	    {
	      case "full" =>
	        var neighborId:Int = findRandomNeighborForFull(numNodes, actorId) 
	        //Send the rumor only if that neighbor is alive
	        if(isAlive(neighborId))
	          originalActors(neighborId) ! Rumor
	        
	      case "2D" =>
	        var neighborId:Int  = findRandomNeighborForTwoD(numNodes, actorId, false)      
	        //Send the rumor only if that neighbor is alive
	        if(isAlive(neighborId))
	          originalActors(neighborId) ! Rumor
	        
	      case "line" =>
	        var neighborId:Int  = findRandomNeighborForLine(numNodes, actorId)
	        //Send the rumor only if that neighbor is alive
	        if(isAlive(neighborId))
	          originalActors(neighborId) ! Rumor
	      
	      case "imp2D" =>
	        var neighborId:Int  = findRandomNeighborForTwoD(numNodes, actorId, true)       
	        //Send the rumor only if that neighbor is alive
	        if(isAlive(neighborId))
	          originalActors(neighborId) ! Rumor
	    }
	    
	    //Schedule for another reminder
	    import system.dispatcher
	    val fd = scala.concurrent.duration.FiniteDuration(1, "milliseconds")
	    context.system.scheduler.scheduleOnce(fd, self, Remind)
	    
      //Gossip - End
	    
      //Push-Sum - Start
	    
      case PushRumor(s, w) =>
        noOfMsgsReceived = noOfMsgsReceived + 1
      	sValue = sValue + s
	    wValue = wValue + w
      	
      	if(noOfMsgsReceived == 1)
      	{
      	  //Start sending push-sum to neighbors
      	  self ! PushRemind()
      	}
	    if(math.abs(previousSWRatio - sValue/wValue) <= math.pow(10,-10))
	    {
         roundCount = roundCount  + 1
        }
	    else
	    {
	      //Reset the round-count if it is not in 3 consecutive rounds 
	      roundCount = 0
	    }
	    //Calculate and store the current s/w ratio
	    previousSWRatio = sValue/wValue 
      	//Check for convergence
      	if(roundCount == 3)
      	{   	  
      	  //Update the isAlive array
          isAlive(actorId) = false;
          master ! PushConverge(previousSWRatio)
          
      	  //Self-destruct
      	  context.stop(self)
      	}     	
      	
	  case PushRemind() =>
	    topology match
	    {
	      case "full" =>
	        var neighborId:Int = findRandomNeighborForFull(numNodes, actorId)
	        //Send the push-sum only if that neighbor is alive
	        if(isAlive(neighborId))
	        {
	          //Divide the sum and weight by 2, only if you are sending push-sum to a neighbor which is alive
	          sValue = sValue/2
	          wValue = wValue/2
	          originalActors(neighborId) ! PushRumor(sValue, wValue)
	        }
	        
	      case "2D" =>
	        var neighborId:Int  = findRandomNeighborForTwoD(numNodes, actorId, false)      
	        //Send the push-sum only if that neighbor is alive
	        if(isAlive(neighborId))
	        {
	          //Divide the sum and weight by 2, only if you are sending push-sum to a neighbor which is alive
	          sValue = sValue/2
	          wValue = wValue/2
	          originalActors(neighborId) ! PushRumor(sValue, wValue)
	        }
	        
	      case "line" =>
	        var neighborId:Int  = findRandomNeighborForLine(numNodes, actorId)
	        //Send the push-sum only if that neighbor is alive
	        if(isAlive(neighborId))
	        {
	          //Divide the sum and weight by 2, only if you are sending push-sum to a neighbor which is alive
	          sValue = sValue/2
	          wValue = wValue/2
	          originalActors(neighborId) ! PushRumor(sValue, wValue)
	        }
	      
	      case "imp2D" =>
	        var neighborId:Int  = findRandomNeighborForTwoD(numNodes, actorId, true)       
	        //Send the push-sum only if that neighbor is alive
	        if(isAlive(neighborId))
	        {
	          //Divide the sum and weight by 2, only if you are sending push-sum to a neighbor which is alive
	          sValue = sValue/2
	          wValue = wValue/2
	          originalActors(neighborId) ! PushRumor(sValue, wValue)
	        }
	    }
	    
	    //Schedule for another reminder
	    import system.dispatcher
	    val fd = scala.concurrent.duration.FiniteDuration(1, "milliseconds")
	    context.system.scheduler.scheduleOnce(fd, self, PushRemind())
	    
	    //Push-Sum - End
    }
  }
    
  def findRandomNeighborForFull(numNodes:Int, actorId:Int):Int =
  {
    var random:Int = Random.nextInt(numNodes)
    if(random == actorId)
    {
      random = random + 1
      //Check for boundary condition
      if(random == numNodes)
        random = random - 2
    }
    return random
  }
    
  def findRandomNeighborForTwoD(numNodes:Int, actorId:Int, isImp2d:Boolean):Int  = 
  {
	  var sqroot = (Math.sqrt(numNodes))
	    
	  //First Node
	  if(actorId == 0)
	  {
	    var numNeighbors:Int = 2
	    if(isImp2d)
	      numNeighbors = 3
	    val neighbors:Array[Int]= new Array[Int](numNeighbors)
	    neighbors(0) = actorId + 1
	    neighbors(1) = actorId + sqroot.intValue
	    if(isImp2d)
	      neighbors(2) = findRandomNeighborForFull(numNodes, actorId)
	     
	    var random:Int = Random.nextInt(numNeighbors)
	    return neighbors(random) 	
	  }
	    
	  //UpperRight border Node
	  else if(actorId == ((numNodes/sqroot)-1))
	  {
	    var numNeighbors:Int = 2
	    if(isImp2d)
	      numNeighbors = 3
	    val neighbors:Array[Int]= new Array[Int](numNeighbors)
	    neighbors(0) = actorId - 1
	    neighbors(1) = actorId + sqroot.intValue
	    if(isImp2d)
	      neighbors(2) = findRandomNeighborForFull(numNodes, actorId)
	     
	    var random:Int = Random.nextInt(numNeighbors)
	    return neighbors(random)
	  }
	    
	  //BottomLeft Border Node
	  else if(actorId == (numNodes-sqroot))
	  {
	    var numNeighbors:Int = 2
	    if(isImp2d)
	      numNeighbors = 3
	    val neighbors:Array[Int]= new Array[Int](numNeighbors)
	    neighbors(0) = actorId + 1
	    neighbors(1) = actorId - sqroot.intValue
	    if(isImp2d)
	      neighbors(2) = findRandomNeighborForFull(numNodes, actorId)
	     
	    var random:Int = Random.nextInt(numNeighbors)
	    return neighbors(random)
	  }
	    
	  //BottomRight Border Node
	  else if(actorId == (numNodes-1))
	  {  
	    var numNeighbors:Int = 2
	    if(isImp2d)
	      numNeighbors = 3
	    val neighbors:Array[Int]= new Array[Int](numNeighbors)
	    neighbors(0) = actorId - 1
	    neighbors(1) = actorId - sqroot.intValue
	    if(isImp2d)
	      neighbors(2) = findRandomNeighborForFull(numNodes, actorId)
	     
	    var random:Int = Random.nextInt(numNeighbors)
	    return neighbors(random)
	  }
	    
	  //Nodes at the top border
	  else if(actorId < ((numNodes/sqroot)-1))
	  {
	    var numNeighbors:Int = 3
	    if(isImp2d)
	      numNeighbors = 4
	    val neighbors:Array[Int]= new Array[Int](numNeighbors)
	    neighbors(0) = actorId + 1
	    neighbors(1) = actorId - 1
	    neighbors(2) = actorId + sqroot.intValue
	    if(isImp2d)
	      neighbors(3) = findRandomNeighborForFull(numNodes, actorId)
	     
	    var random:Int = Random.nextInt(numNeighbors)
	    return neighbors(random) 
	  }
	    
	  //Nodes at the bottom border
	  else if(actorId > (numNodes-sqroot) && actorId < (numNodes-1))
	  {
	    var numNeighbors:Int = 3
	    if(isImp2d)
	      numNeighbors = 4
	    val neighbors:Array[Int]= new Array[Int](numNeighbors)
	    neighbors(0) = actorId + 1
	    neighbors(1) = actorId - 1
	    neighbors(2) = actorId - sqroot.intValue
	    if(isImp2d)
	      neighbors(3) = findRandomNeighborForFull(numNodes, actorId)
	     
	    var random:Int = Random.nextInt(numNeighbors)
	    return neighbors(random)
	  }
	    
	  //Nodes at the left border
	  else if((actorId % sqroot) == 0)
	  {
	    var numNeighbors:Int = 3
	    if(isImp2d)
	      numNeighbors = 4
	    val neighbors:Array[Int]= new Array[Int](numNeighbors)
	    neighbors(0) = actorId + 1
	    neighbors(1) = actorId - sqroot.intValue
	    neighbors(2) = actorId + sqroot.intValue
	    if(isImp2d)
	      neighbors(3) = findRandomNeighborForFull(numNodes, actorId)
	     
	    var random:Int = Random.nextInt(numNeighbors)
	    return neighbors(random)
	  }
	    
	  //Nodes at the right border
	  else if(((actorId+1) % sqroot) == 0)
	  {
	    var numNeighbors:Int = 3
	    if(isImp2d)
	      numNeighbors = 4
	    val neighbors:Array[Int]= new Array[Int](numNeighbors)
	    neighbors(0) = actorId - 1
	    neighbors(1) = actorId - sqroot.intValue
	    neighbors(2) = actorId + sqroot.intValue
	    if(isImp2d)
	      neighbors(3) = findRandomNeighborForFull(numNodes, actorId)
	     
	    var random:Int = Random.nextInt(numNeighbors)
	    return neighbors(random)
	  }
	    
	  //Nodes inside the grid
	  else
	  {
	    var numNeighbors:Int = 4
	    if(isImp2d)
	      numNeighbors = 5
	    val neighbors:Array[Int]= new Array[Int](numNeighbors)
	    neighbors(0) = actorId + 1
	    neighbors(1) = actorId - 1
	    neighbors(2) = actorId - sqroot.intValue
	    neighbors(3) = actorId + sqroot.intValue
	    if(isImp2d)
	      neighbors(4) = findRandomNeighborForFull(numNodes, actorId)
	     
	    var random:Int = Random.nextInt(numNeighbors)
	    return neighbors(random)
	  }
  }

  def findRandomNeighborForLine(numNodes:Int, actorId:Int):Int = 
  {         
    //First Node
    if(actorId == 0)
      return actorId + 1
     
    //Last Node
    else if(actorId == (numNodes-1))
      return actorId - 1
     
    //Remaining Nodes
    else
    {
      var neighbors:Array[Int] = new Array[Int](2)
      neighbors(0) = actorId + 1
      neighbors(1) = actorId - 1
      var random:Int = Random.nextInt(2)   
      return neighbors(random)
    } 
  }
}
