import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.ActorSelection
import scala.concurrent.{Await, ExecutionContext, Future}
import akka.pattern.ask
import akka.util.Timeout
import scala.language.postfixOps
import scala.concurrent.duration._
import java.lang.Math
import com.typesafe.config.ConfigFactory
import java.security.MessageDigest
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.List
import java.util.Random
import java.util.concurrent.TimeUnit
import java.math.BigInteger

import scala.util.control.Breaks._

object Chord {
	val M = 160
  //val M = 3
  var testKeys: Array[Int] = new Array[Int](4)
	val UNSIGNED = 1
	val INT_SIZE = 4
	var next_hops: Double = 0
  var r: Int = 0
  var successfulMessages = 0
	case object Start
	case class SetFingerTable(row : Int, node : ActorRef)
	case class SetPredecessor(node : ActorRef) 
	case class SetSuccessor(node : ActorRef)	
	case class FindSuccessor(searchkey : BigInteger)
	case class FindPredecessor(searchkey : BigInteger)
	case class FoundSuccessor(pre : ActorRef, suc : ActorRef)
	case class GetFingerTableStart(row : Int)
  case object PrintFingerTable
  case object GetNodeId
	case class GetFingerTableNode(row : Int)
	case class GetPredecessor()
	case class GetSuccessor()
  case class SetSuccessorList(rNodeId: Int, row : Int)
  case class GetSuccessorList(row : Int)
  case class StartMessaging()
	case class UpdateFingerTable(node : ActorRef, i : Int)
	case object Stop

	def main(args : Array[String]) {
    
		var (numOfNodes, numOfRequests,deadFactor) = readArgs(args)
    var rnd : Random = new Random()
    r = (Math.log(numOfNodes)/Math.log(2)).toInt
    
    testKeys(0) = 0 
    testKeys(1) = 3
    testKeys(2) = 1
    testKeys(3) = 6

		val systemCongfig = ActorSystem("Chord",ConfigFactory.load())	
		var master = systemCongfig.actorOf(Props(classOf[Master], numOfNodes, numOfRequests,deadFactor), "Master")
		master ! Start

    /*
    val tmp = "1"
    println("Testing isOpenBelongTo")
    println("1 is open belong to 0 and 2, the result is: "+isOpenBelongTo(new BigInteger(tmp), "0", "2"))
    println("1 is open belong to 1 and 2, the result is: "+isOpenBelongTo(new BigInteger(tmp), "1", "2"))
    println("1 is open belong to 0 and 1, the result is: "+isOpenBelongTo(new BigInteger(tmp), "0", "1"))
    println("1 is open belong to 1 and 1, the result is: "+isOpenBelongTo(new BigInteger(tmp), "1", "1"))
    println("1 is open belong to 2 and 3, the result is: "+isOpenBelongTo(new BigInteger(tmp), "2", "3"))
    println("1 is open belong to 3 and 0, the result is: "+isOpenBelongTo(new BigInteger(tmp), "3", "0"))
    println("1 is open belong to 2 and 2, the result is: "+isOpenBelongTo(new BigInteger(tmp), "2", "2"))
    println("1 is open belong to 5 and 3, the result is: "+isOpenBelongTo(new BigInteger(tmp), "5", "3"))
    println("Testing isBelongTo")
    println("1 is belong to 0 and 2, the result is: "+isBelongTo(new BigInteger(tmp), "0", "2"))
    println("1 is belong to 1 and 2, the result is: "+isBelongTo(new BigInteger(tmp), "1", "2"))
    println("1 is belong to 0 and 1, the result is: "+isBelongTo(new BigInteger(tmp), "0", "1"))
    println("1 is belong to 1 and 1, the result is: "+isBelongTo(new BigInteger(tmp), "1", "1"))
    println("1 is belong to 2 and 3, the result is: "+isBelongTo(new BigInteger(tmp), "2", "3"))
    println("1 is belong to 3 and 0, the result is: "+isBelongTo(new BigInteger(tmp), "3", "0"))
    println("1 is belong to 2 and 2, the result is: "+isBelongTo(new BigInteger(tmp), "2", "2"))
    println("1 is belong to 5 and 3, the result is: "+isBelongTo(new BigInteger(tmp), "5", "3"))
    println("Testing isClosedBelongTo")
    println("1 is closed belong to 0 and 2, the result is: "+isClosedBelongTo(new BigInteger(tmp), "0", "2"))
    println("1 is closed belong to 1 and 2, the result is: "+isClosedBelongTo(new BigInteger(tmp), "1", "2"))
    println("1 is closed belong to 0 and 1, the result is: "+isClosedBelongTo(new BigInteger(tmp), "0", "1"))
    println("1 is closed belong to 1 and 1, the result is: "+isClosedBelongTo(new BigInteger(tmp), "1", "1"))
    println("1 is closed belong to 2 and 3, the result is: "+isClosedBelongTo(new BigInteger(tmp), "2", "3")) 
    println("1 is closed belong to 3 and 0, the result is: "+isClosedBelongTo(new BigInteger(tmp), "3", "0"))
    println("1 is closed belong to 2 and 2, the result is: "+isClosedBelongTo(new BigInteger(tmp), "2", "2"))
    println("1 is closed belong to 5 and 3, the result is: "+isClosedBelongTo(new BigInteger(tmp), "5", "3"))
    println("Testing isNotBelongTo")
    println("1 is not belong to 0 and 2, the result is: "+isNotBelongTo(new BigInteger(tmp), "0", "2"))
    println("1 is not belong to 1 and 2, the result is: "+isNotBelongTo(new BigInteger(tmp), "1", "2"))
    println("1 is not belong to 0 and 1, the result is: "+isNotBelongTo(new BigInteger(tmp), "0", "1"))
    println("1 is not belong to 1 and 1, the result is: "+isNotBelongTo(new BigInteger(tmp), "1", "1"))
    println("1 is not belong to 2 and 3, the result is: "+isNotBelongTo(new BigInteger(tmp), "2", "3")) 
    println("1 is not belong to 3 and 0, the result is: "+isNotBelongTo(new BigInteger(tmp), "3", "0"))
    println("1 is not belong to 2 and 2, the result is: "+isNotBelongTo(new BigInteger(tmp), "2", "2"))
    println("1 is not belong to 5 and 3, the result is: "+isNotBelongTo(new BigInteger(tmp), "5", "3"))
	  println("0 is not belong to 3 and 0, the result is: "+isNotBelongTo(new BigInteger("0"), "3", "0"))
    println("0 is not belong to 0 and 3, the result is: "+isNotBelongTo(new BigInteger("0"), "0", "3"))
   */
  }

  def readArgs(args : Array[String]) : (Int, Int,Double) = {
  	var numOfNodes = args(0).toInt
  	var numOfRequests = args(1).toInt
    var deadFactor = args(2).toDouble
  	return(numOfNodes, numOfRequests,deadFactor)
  }

  def isOpenBelongTo(node : BigInteger, left : String, right : String) : Boolean =  {
      var leftInt = new BigInteger(left)
      var rightInt = new BigInteger(right)
      if(leftInt.compareTo(rightInt) == -1){
        if(node.compareTo(leftInt) == 1 && node.compareTo(rightInt) == -1) {
          return true
        }
      }else if(leftInt.compareTo(rightInt) == 1){
        var max = BigInteger.valueOf(2).pow(M)
        if(node.compareTo(leftInt) == 1 && node.compareTo(max) == -1) {
          return true;
        }
        if(node.compareTo(BigInteger.valueOf(0)) >= 0 && node.compareTo(rightInt) == -1) {
          return true;
        }
      }else{
        if(node.compareTo(leftInt)!=0)return true
      }
      return false;
  }

  def isBelongTo(node : BigInteger, left : String, right : String) : Boolean =  {
    var leftInt = new BigInteger(left)
    var rightInt = new BigInteger(right)
    if(leftInt.compareTo(rightInt) == 0){
      return true
    }
      
    if(node.equals(leftInt)){return true}
    else {return isOpenBelongTo(node, left, right) }
  }
  def isClosedBelongTo(node : BigInteger, left : String, right : String) : Boolean =  {
    var leftInt = new BigInteger(left)
    var rightInt = new BigInteger(right)
    if(leftInt.compareTo(rightInt) == 0){
      return true
    }
      
    if(node.equals(leftInt)){return true}
    else if(node.equals(rightInt)){return true}
    else {return isOpenBelongTo(node, left, right) }
  }

  def isNotBelongTo(node : BigInteger, left : String, right : String) : Boolean = {
    var leftInt = new BigInteger(left)
    var rightInt = new BigInteger(right)
    if(isOpenBelongTo(node, left, right)||node.compareTo(rightInt) == 0){
      return false
    }
    if(leftInt.compareTo(rightInt) == 0){
      return false
    }
    return true
  }


  class Master(numOfNodes: Int, numOfRequests: Int, deadFactor: Double) extends Actor {
    var nodes : Array[ActorRef] = new Array(numOfNodes)
    var deadNodes : Array[Boolean] = new Array(numOfNodes)
    def receive = {
    	case Start => {
    		System.out.println("The program begin with " +numOfNodes+" nodes and " + numOfRequests +" request")
    		System.out.println("Building network ......")   
    		System.out.println("Master is " + self.path.name) 		
    		BuildStructure()   
        BuildSuccessorList() 
        System.out.println("Chord network build finished.")
        KillSome(deadFactor)
        val system = context.system
        import system.dispatcher
        system.scheduler.scheduleOnce(Duration(1000, TimeUnit.MILLISECONDS), self, StartMessaging)
    	}
      case StartMessaging => {
        implicit val timeout = Timeout(5 seconds)
        next_hops = 0
        System.out.println("Start messaging")

        for(i <- 0 to numOfRequests - 1){
          for(j <- 0 to numOfNodes - 1) {
            System.out.print("Send message to Node "+ j+" with message Haha!!\n")
            var rnd : Random = new Random();
            var randomNode =  new BigInteger(M,rnd)
            var future = ask(nodes(j), FindSuccessor(randomNode)).mapTo[Array[ActorRef]]
            var presuc = Await.result(future, 5 seconds) 
            var tar = presuc(1)
            var futureIdx = ask(tar, GetNodeId).mapTo[Int]
            var tarIdx = Await.result(futureIdx, 5 seconds) 
            var rowIdx = 0
            while(rowIdx+1< r && tarIdx>=0 && 
              tarIdx< numOfNodes-1 && deadNodes(tarIdx)==true){
              rowIdx=rowIdx+1
              futureIdx = ask(nodes(j), GetSuccessorList(rowIdx) ).mapTo[Int]
              tarIdx = Await.result(futureIdx, 5 seconds) 
            }
            if(deadNodes(tarIdx)==true)System.out.println("Failed to send message to Node "+j+"!")
            else{
              successfulMessages+=1 
              System.out.println("Node "+j+" Received message haha!")
              } 
            }
          }

        var successfulRate:Double =  successfulMessages.toDouble/(numOfRequests*numOfNodes)
        System.out.println("Message done, with avergae next_hops of "+next_hops/(numOfRequests*numOfNodes))
        System.out.println("With successorList length of r: "+ r +" We got Successful rate: "+successfulRate)
        context.system.shutdown()
        }


    }
    def KillSome(deadFactor: Double)={
      var max = (numOfNodes * deadFactor).toInt
      for(i <- 0 to max-1) {
        deadNodes(i) = true
      }
    }
    def PrintStructure()={
      implicit val timeout = Timeout(5 seconds)
      for(i <- 0 to numOfNodes - 1) {
        var future = ask(nodes(i), PrintFingerTable )
        var n = Await.result(future, 5 seconds)
      }
    }
    def BuildStructure() = {

    	for(i <- 0 to numOfNodes - 1) {
          val md = MessageDigest.getInstance("SHA-1")
		      val bytes : Array[Byte] = ByteBuffer.allocate(INT_SIZE).putInt(i).array()
          /*var rnd : Random = new Random()
          var key =  new BigInteger(M,rnd)*/
		      var key : BigInteger = new BigInteger(UNSIGNED, md.digest(bytes))
          //var key =  new BigInteger(new Integer(testKeys(i)).toString())
		      System.out.println("Key of node " + i + " is " + key.toString())
          nodes(i) = context.actorOf(Props(classOf[Node],i, key), name = key.toString())           
      }
      for(i <- 0 to M - 1) {
       	nodes(0) ! SetFingerTable(i, nodes(0))        	
    	}
    	nodes(0) ! SetPredecessor(nodes(0))
    	nodes(0) ! SetSuccessor(nodes(0))
      System.out.println("")
      
    	for(i <- 1 to numOfNodes - 1) {
       	initFingerTable(i)
        updateOthers(i)	
      }
    }

    def BuildSuccessorList() = {
      implicit val timeout = Timeout(5 seconds)
      for(i <- 0 to numOfNodes - 1) {
        var suc: ActorRef = nodes(i)
        var sucIdx: Int = -1
        for(j<- 0 to r-1){
          var future = ask(suc, GetSuccessor).mapTo[ActorRef]
          suc = Await.result(future, 5 seconds)
          var futureIdx = ask(suc, GetNodeId).mapTo[Int]
          sucIdx = Await.result(futureIdx, 5 seconds)
          nodes(i)!SetSuccessorList(sucIdx,j)
        }
      }
    }
    

    def initFingerTable(nodeId : Int) = {
    	implicit val timeout = Timeout(5 seconds)
    	var future1 : Future[BigInteger] = ask(nodes(nodeId), GetFingerTableStart(0)).mapTo[BigInteger]
    	val start = Await.result(future1, 5 seconds)
    	//println("Get start " + start)
    	//println("who send FindSuccessor : " + self.path.name)
    	var future2 = ask(nodes(0), FindSuccessor(start)).mapTo[Array[ActorRef]]
    	val successor = Await.result(future2, 10 seconds)
    	//println("Get Successor " + successor)
    	nodes(nodeId) ! SetSuccessor(successor(1))
    	nodes(nodeId) ! SetFingerTable(0, successor(1))
    	val future3 : Future[ActorRef] = ask(successor(1), GetPredecessor).mapTo[ActorRef]
    	val predecessor = Await.result(future3, 5 seconds)
    	//println("Get predecessor " + predecessor.path.name)
    	nodes(nodeId) ! SetPredecessor(predecessor)
      successor(1) ! SetPredecessor(nodes(nodeId))
    	for(i <- 0 to M - 2) {
    		var futureForStart = ask(nodes(nodeId), GetFingerTableStart(i + 1)).mapTo[BigInteger]
    		var futureForNode = ask(nodes(nodeId), GetFingerTableNode(i)).mapTo[ActorRef]
    		var startI = Await.result(futureForStart, 5 seconds)
    		var nodeI = Await.result(futureForNode, 5 seconds)
    		
    		if(isClosedBelongTo(startI, nodes(nodeId).path.name, nodeI.path.name)) {
          //
    			nodes(nodeId) ! SetFingerTable(i + 1, nodeI)
    		} else {
    			var futureForSuccessor = ask(nodes(0), FindSuccessor(startI)).mapTo[Array[ActorRef]]
    			var suc = Await.result(futureForSuccessor, 5 seconds)
          //println("Get successor for start: "+startI+", the result is " + suc(0).path.name)
    			nodes(nodeId) ! SetFingerTable(i + 1, suc(1))
    		}
    	}
    	//println("Set fingerTable for node " + nodeId)
    }

    def updateOthers(nodeId : Int) {
    	implicit val timeout = Timeout(5 seconds)
      //println("I want to update " + nodeId)
    	for(i <- 0 to M -1) {
    		var key = new BigInteger(nodes(nodeId).path.name)
        var zeros = new BigInteger("0");
        var target = key.add(BigInteger.valueOf(2).pow(i).negate())
        if(target.compareTo(zeros) == -1){
          target = target.add(BigInteger.valueOf(2).pow(M))
        }

        
    		var future = ask(nodes(nodeId), FindPredecessor(target)).mapTo[Array[ActorRef]]
    		var pre = Await.result(future, 5 seconds)
        //println("I am looking in Node: "+nodeId+" for target: " + target+", The predecessor I found is:"+ pre(0).path.name)
    		var f = ask(pre(0) , UpdateFingerTable(nodes(nodeId), i)).mapTo[ActorRef]
        var p = Await.result(f, 5 seconds)
       //println("Update finish")
    	}
    }

  }

  class Node(nodeId : Int, key : BigInteger) extends Actor {
  	var predecessor : ActorRef = null
  	var successor : ActorRef = null
  	var fingerTable : Array[Record] = new Array(M)
    var successorList: Array[Int] = new Array(r)
  	implicit val timeout = Timeout(5 seconds)
  	  	
  	fingerTable(0) = new Record()
  	fingerTable(0).start = key.add(BigInteger.valueOf(1)).mod(BigInteger.valueOf(2).pow(M))
  	fingerTable(0).end = key.add(BigInteger.valueOf(2)).mod(BigInteger.valueOf(2).pow(M))
  	for(i <- 1 to M - 1) {
		  fingerTable(i) = new Record()
		  fingerTable(i).start = fingerTable(i - 1).end
		  fingerTable(i).end = key.add(BigInteger.valueOf(2).pow(i + 1)).mod(BigInteger.valueOf(2).pow(M))
	  }
	//println("Set table for node " + nodeId)

	def receive = {
		case SetFingerTable(row : Int, node : ActorRef) => {
			fingerTable(row).node = node 
			//println("Set table success")
		}

    case GetNodeId => {
      sender ! nodeId
    }
		case UpdateFingerTable(node : ActorRef, i : Int) => {
			var value = new BigInteger(node.path.name)
			if(isOpenBelongTo(value, key.toString, fingerTable(i).node.path.name)) {  
				fingerTable(i).node = node
        if(i==0)successor = node
        predecessor forward UpdateFingerTable(node, i)
			}
      sender ! successor
		}
    case PrintFingerTable => {
        println("I'm Node "+ key)
        if(predecessor == null){
          println("My predecessor is null\n")
        }else{
          print("My predecessor is " +predecessor.path.name+"\n")
        }
        if(successor == null){
          println("My successor is null\n")
        }else{
          print("My successor is " +successor.path.name +"\n")
        }
        for(i <- 0 to M -1) {
          if(fingerTable(i).node==null){
            print("FingerTable inteval start "+fingerTable(i).start+" of Node " + nodeId +" row: "+i+" null\n")
          }else{
            print("FingerTable inteval start "+fingerTable(i).start+" of Node " + fingerTable(i).node.path.name +"\n")
          }
        }
        sender ! "haha"
    }

    case SetSuccessorList(rNodeId: Int, row : Int) => {
      successorList(row) = rNodeId
    }
    case GetSuccessorList(row : Int) => {
      sender ! successorList(row)
    }
		case SetPredecessor(node : ActorRef) => {
			predecessor = node
		}
		case GetPredecessor => {
			sender ! predecessor
		}
		case SetSuccessor(node : ActorRef) => {
			successor = node
			//println("Set suc success")
		}
		case GetSuccessor => {
			sender ! successor
		}
		case GetFingerTableStart(row : Int) => {
			sender ! fingerTable(row).start
		}
		case GetFingerTableNode(row : Int) => {
			sender ! fingerTable(row).node
		}
		case FindSuccessor(searchkey : BigInteger) => {
			self forward FindPredecessor(searchkey)
		}

		case FindPredecessor(searchkey : BigInteger) => {
      //println("Looking for predecessor for searchKey: "+ searchkey + " in node: " + key)
      var res: Array[ActorRef] = new Array(2)
     // print("check if the searchKey: "+searchkey+" is not belong to "+cur.path.name+" and "+curSuccessor.path.name+ " the result is: "
       // +isNotBelongTo(searchkey, cur.path.name, curSuccessor.path.name)+" \n")
      
      next_hops+=1
			if(isNotBelongTo(searchkey, self.path.name, successor.path.name)){
				//print("I found "+searchkey+" is not belong to "+cur.path.name+" and "+curSuccessor.path.name+ " \n")
				var cur = closestPrecedingFinger(searchkey)
				if(cur != self){
          //println("Transfer to "+cur.path.name+" to find searchkey: "+searchkey)    
          var future1 = ask(cur, FindPredecessor(searchkey)).mapTo[Array[ActorRef]]
          res = Await.result(future1, 5 seconds)
				}				
			}

      if(res(0)==null){
        res(0) = self
        res(1) = successor
      }
      //println("Try to find predecessor for searchKey: "+ searchkey + " return me: "+res(0).path.name
      //  +" and my successor: " + res(1).path.name)
			sender ! res
	}
		
}

	def closestPrecedingFinger(searchkey : BigInteger) : ActorRef = {
		for(i<- M - 1 to 0 by -1){
			var nodeI = new BigInteger(fingerTable(i).node.path.name)
     // print("check if the nodeI: "+nodeI+" is open belong to "+key.toString+" and "+searchkey.toString+ " the result is: "
      //  +isOpenBelongTo(nodeI, key.toString, searchkey.toString)+" \n")
			if(isOpenBelongTo(nodeI, key.toString, searchkey.toString)) {
				return fingerTable(i).node
			}
		}
		return self;
	}
}

  class Record(){
  	var start : BigInteger = null
  	var end : BigInteger = null
  	var node : ActorRef = null
  }
  
  
}





