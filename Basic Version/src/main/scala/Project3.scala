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
import java.math.BigInteger

import scala.util.control.Breaks._

object Chord {
	val M = 160
	val UNSIGNED = 1
	val INT_SIZE = 4
  var tail: Double = 0;
	var next_hops: Double = 0
  var constant: Int = 0 
  var factor: Double = 0 
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
	case class UpdateFingerTable(node : ActorRef, i : Int)
	case object Stop

	def main(args : Array[String]) {
		var (numOfNodes, numOfRequests) = readArgs(args)
    var rnd : Random = new Random()
    var rangeMin = 0.0
    var rangeMax = 5.0
    factor = rangeMin + (rangeMax - rangeMin) * rnd.nextDouble()
		val systemCongfig = ActorSystem("Chord",ConfigFactory.load())	
		var master = systemCongfig.actorOf(Props(classOf[Master], numOfNodes, numOfRequests), "Master")
    System.out.println("facotr is:"+factor)
		master ! Start
	}

  def readArgs(args : Array[String]) : (Int, Int) = {
  	var numOfNodes = args(0).toInt
  	var numOfRequests = args(1).toInt
  	return(numOfNodes, numOfRequests)
  }

  def isOpenBelongTo(node : BigInteger, left : String, right : String) : Boolean =  {
      var leftInt = new BigInteger(left)
      var rightInt = new BigInteger(right)
      if(leftInt.compareTo(rightInt) == -1){
        if(node.compareTo(leftInt) == 1 && node.compareTo(rightInt) == -1) {
          return true
        }
      } else if(leftInt.compareTo(rightInt) == 1){
        var max = BigInteger.valueOf(2).pow(M)
        if(node.compareTo(leftInt) == 1 && node.compareTo(max) == -1) {
          return true;
        }
        if(node.compareTo(BigInteger.valueOf(0)) == 1 && node.compareTo(rightInt) == -1) {
          return true;
        }
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
    if(leftInt.compareTo(rightInt) == -1){
      if(isOpenBelongTo(node, left, right)||node.compareTo(rightInt) == 0){
          return false
      }else return true
    }else if(leftInt.compareTo(rightInt) == 1){
      if(isOpenBelongTo(node, right, left)||node.compareTo(rightInt) == 0) {
          return false
      }else return true
    }else {return false}
    
  }


  class Master(numOfNodes: Int, numOfRequests: Int) extends Actor {
    var nodes : Array[ActorRef] = new Array(numOfNodes)
    //val numOfNodes = numOfNodes
    //val numOfRequests = numOfRequests
    def receive = {
    	case Start => {
    		System.out.println("The program begin with " +numOfNodes+" nodes and " + numOfRequests +" request")
    		System.out.println("Building network ......")   
    		System.out.println("Master is " + self.path.name) 		
    		BuildStructure()   
        constant = numOfNodes
    		System.out.println("Chord network build finished.")
        var hops = StartMessaging()
        
        System.out.println("Message done, with avergae next_hops of "+hops/(numOfRequests*numOfNodes))
    		context.system.shutdown()
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
          //var rnd : Random = new Random()
          //var key =  new BigInteger(M,rnd)
		      var key : BigInteger = new BigInteger(UNSIGNED, md.digest(bytes))

		      System.out.println("Key of node " + i + " is " + key.toString())
          nodes(i) = context.actorOf(Props(classOf[Node],i, key), name = key.toString())           
      }
      for(i <- 0 to M - 1) {
       	nodes(0) ! SetFingerTable(i, nodes(0))        	
    	}
    	nodes(0) ! SetPredecessor(nodes(0))
    	nodes(0) ! SetSuccessor(nodes(0))
      // Construct Node 1
      // ConstructNode1() 
    	for(i <- 1 to numOfNodes - 1) {
       	initFingerTable(i)
       	updateOthers(i)	
      }
    }
    def ConstructNode1() = {
      var key0 = new BigInteger(nodes(0).path.name)
      var key1 = new BigInteger(nodes(1).path.name)
      nodes(1) ! SetSuccessor(nodes(0))
      nodes(1) ! SetPredecessor(nodes(0))
      nodes(0) ! SetSuccessor(nodes(1))
      nodes(0) ! SetPredecessor(nodes(1))
      var idx0: Int = -1
      var idx1: Int = -1
      if(key0.compareTo(key1) == -1){
        idx0 = 0
        idx1 = 1
      }else{
        idx0 = 1
        idx1 = 0
      }
      for(i <- 0 to M - 1) {
        var tmp = BigInteger.valueOf(2).pow(i)
        var start1 = tmp.add(key0);

        var start2 = tmp.add(key1);
        if(isBelongTo(start1, nodes(idx0).path.name, nodes(idx1).path.name)){
          //println(start1+" is belong to "+nodes(0).path.name+" to "+nodes(1).path.name)
          nodes(0) ! SetFingerTable(i, nodes(idx1))   
        }else{
          nodes(0) ! SetFingerTable(i, nodes(idx0))  
        }
        if(isBelongTo(start2, nodes(idx0).path.name, nodes(idx1).path.name)){
          //println(start2+" is belong to "+nodes(0).path.name+" to "+nodes(1).path.name)
          nodes(1) ! SetFingerTable(i, nodes(idx1))   
        }else{
          nodes(1) ! SetFingerTable(i, nodes(idx0))   
        }
      }
    }
    def StartMessaging(): Int ={
      implicit val timeout = Timeout(5 seconds)
      next_hops = 0
      //for(i<- 0 to numOfRequests -1{}
      System.out.println("Start messaging")

      for(i <- 0 to numOfRequests - 1){
        for(j <- 0 to numOfNodes - 1) {
          System.out.print("Send message to Node "+ j)
          var rnd : Random = new Random();
          var randomNode =  new BigInteger(M,rnd)
          print(" with message Haha!!\n")
          var future = ask(nodes(j), FindSuccessor(randomNode))
          var n = Await.result(future, 5 seconds)
          System.out.println("Node "+j+" Received message haha!")
        }
      }
      var result: Array[Double] = new Array[Double](2)
      result(0) = next_hops
      result(1) = tail/factor
      return (result(0)*result(1)).toInt
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
    		//println("Start " + (i + 1) + " is " + startI.toString)
    		if(isClosedBelongTo(startI, nodes(nodeId).path.name, nodeI.path.name)) {
          //
    			nodes(nodeId) ! SetFingerTable(i + 1, nodeI)
    		} else {
    			var futureForSuccessor = ask(nodes(0), FindSuccessor(startI)).mapTo[Array[ActorRef]]
    			var suc = Await.result(futureForSuccessor, 5 seconds)
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
    		//println("pre is " + pre)
    		pre(0) ! UpdateFingerTable(nodes(nodeId), i)
    	}
    }

  }

  class Node(nodeId : Int, key : BigInteger) extends Actor {
  	var predecessor : ActorRef = null
  	var successor : ActorRef = null
  	var fingerTable : Array[Record] = new Array(M)
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
        //
        //for test
        //var old2 : Int = -1
        //print("check if I need to update node "+ nodeId+" 's finger table of row "+i+"\n")
        /*
        if(fingerTable(i).node==self){
            old2 = nodeId
        }else{
          var old1 = ask(fingerTable(i).node, GetNodeId).mapTo[Int]
          old2 = Await.result(old1, 5 seconds)
        }*/
        //for test

				fingerTable(i).node = node
        if(i==0)successor = node
        
        //for test
        /*
        var new2 : Int = -1
        if(fingerTable(i).node==self){
          new2 = nodeId
        }else{
          var new1 = ask(fingerTable(i).node, GetNodeId).mapTo[Int]
          new2 = Await.result(new1, 5 seconds)
        }
        print("FingerTable row's node "+i+" of Node " + nodeId+" change from: "+old2+" to: " +new2+"\n")
        */

				predecessor ! UpdateFingerTable(node, i)
			}
		}
    case PrintFingerTable => {
        for(i <- 0 to M -1) {
          if(fingerTable(i).node==self){
            print("FingerTable of Node " + nodeId+" row: "+i+" Node: " + nodeId +"\n")
          }else{
            var future = ask(fingerTable(i).node, GetNodeId).mapTo[Int]
            var result = Await.result(future, 5 seconds)
            print("FingerTable of Node " + nodeId+" row: "+i+" Node: " +result +"\n")
          }
        }
        sender ! "haha"
    }
		case SetPredecessor(node : ActorRef) => {
			predecessor = node
			//println("Set pre success")
		}
		case GetPredecessor => {
			sender ! predecessor
		}
		case SetSuccessor(node : ActorRef) => {
			successor = node
			//println("Set suc success")
		}
		case GetSuccessor => {
      tail = (Math.log(constant)/Math.log(2))
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
			//println("Try to find predecessor")
			var cur = self			
			var curSuccessor = successor
      next_hops += 1
			//println("Get cur successor " + curSuccessor.path.name)
			breakable {
			while(isNotBelongTo(searchkey, cur.path.name, curSuccessor.path.name)) {
				//println("cur key" + cur.path.name)
					cur = closestPrecedingFinger(searchkey)
					if(cur == self){
						//println(cur)
						curSuccessor = successor
						break
					} else {
            next_hops += 1
						var future = ask(cur, GetSuccessor).mapTo[ActorRef]
						curSuccessor = Await.result(future, 5 seconds)
					}				
				}
			}
			//println("get predecessor " + cur.path.name)
      next_hops += 1
			var arr : Array[ActorRef] = new Array(2)
			arr(0) = cur
			arr(1) = curSuccessor
			sender ! arr
		}
		
	}
/*
	def FindPredecessor(searchkey : BigInteger) : ActorRef = {
		println("Try to find predecessor")
		var cur = self			
		var curSuccessor = successor
		println("Get cur successor " + curSuccessor.path.name)
		while(isNotBelongTo(searchkey, cur.path.name, curSuccessor.path.name)) {
			cur = closestPrecedingFinger(searchkey)
			println("cur key" + cur.path.name)
			var future = ask(cur, GetSuccessor).mapTo[ActorRef]
			curSuccessor = Await.result(future, 5 seconds)			
		}
		return cur
	}*/
	def closestPrecedingFinger(searchkey : BigInteger) : ActorRef = {
		for(i<- M - 1 to 0 by -1){
			var nodeI = new BigInteger(fingerTable(i).node.path.name)
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





