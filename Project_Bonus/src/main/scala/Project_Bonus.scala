import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import akka.actor.ActorRef
import java.util.Arrays
import scala.util.Sorting
import scala.util.control.Breaks._
import akka.dispatch.Foreach
import akka.actor.Cancellable
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.io.PrintWriter
import java.io.File
import akka.actor.Identify
import akka.actor.ActorIdentity
import akka.actor.Terminated

case object Start
case object Stop
case class StartJoin(neighbourKey: Int)
case object JoinFinished
case class Join(key: Int)
case class Welcome(nodeState: PastryNodeState, numRowsRoutingTable: Int)
case class UpdateState(nodeState: PastryNodeState)
case object UpdateFinished
case class JoinFinish(nodeState: PastryNodeState)
case class StartMessaging(numMsgs: Int)
case object SendMessage
case class Deliver(msg: String)
//case class Forward(key: Int, sender: Int, msg: String, path: Array[Int], msgCount: Int)
case class Forward(key: Int, sender: Int, msg: String, msgCount: Int, numHops: Int)
case class NodeDead(key: Int, sender: Int, msg: String, msgCount: Int, numHops: Int)
//case class MessageDelivered(path: Array[Int], msgCount: Int)
case class MessageDelivered(numHops: Int, msgCount: Int)
case class MessagingComplete(numHops: Double, numRequests: Int)
case object PrintState
case object FailNode
case object RequestForLeafSet
case object RequestForRoutingTable
case class UpdateLeafSet(nodeState: PastryNodeState)
case class UpdateRoutingTable(nodeState: PastryNodeState)

//TODO - dead node removed from leafset and routing table
//updating leafset and routing table with new live node
object Project_Bonus {
  val DEFAULT_NUM_NODES: Int = 1000
  val DEFAULT_NUM_REQUESTS: Int = 10
  val DEFAULT_NUM_FAILURES: Int = 0

  def main(args: Array[String]) {
    var (numNodes, numRequests, numFailures) = try {
      (args(0).toInt, args(1).toInt, args(2).toInt)
    } catch {
      case ex: Exception => {
        println("Invalid input arguments !!!\nRunning with default configuration (numNodes=1000, numRequest=10, numFailures=0)")
        (DEFAULT_NUM_NODES, DEFAULT_NUM_REQUESTS, DEFAULT_NUM_FAILURES)
      }
    }

    if (numNodes < 2 || numRequests < 1 || (numFailures < 0 && numFailures > numNodes)) {
      println("Invalid input arguments !!!\nRunning with default configuration (numNodes=1000, numRequest=10, numFailures=0)")
      numNodes = DEFAULT_NUM_NODES
      numRequests = DEFAULT_NUM_REQUESTS
    }

    println(numNodes + " " + numRequests + " " + numFailures)

    startPastry(numNodes, numRequests, numFailures)
  }

  def startPastry(numNodes: Int, numRequests: Int, numFailures: Int) {
    val system = ActorSystem("pastry")
    val master = system.actorOf(Props(classOf[Master], numNodes, numRequests, numFailures), name = "master")

    println("Starting Master")
    master ! Start
  }
}

class Master(pNumNodes: Int, pNumRequests: Int, numFailures: Int) extends Actor {
  val numNodes = pNumNodes
  val numRequests = pNumRequests
  //var networkNodes: Array[Int] = new Array(numNodes) //array buffer of size numNodes
  var networkNodes: Array[ActorRef] = new Array(numNodes) //array of size numNodes
  var joinCount = 1
  var msgCount = 0
  var numFailedNodes = 0
  var totalHops: Double = 0.0
  var totalRequests: Int = 0
  var scheduler: Cancellable = null

  def receive = {
    case Start => {
      for (i <- 0 until numNodes) {
        var randomId = NodeId.randomId
        //networkNodes(i) = randomId
        networkNodes(i) = context.actorOf(Props(classOf[PastryNode], randomId), name = randomId.toString)
      }
      //println("Network nodes created: " + Arrays.toString(networkNodes))
      println("Network nodes created")

      println("Starting nodes")
      //println("First node started by default. Starting second node")
      //context.actorSelection(networkNodes(1).toString) ! StartJoin(networkNodes(0))
      networkNodes(1) ! StartJoin(networkNodes(0).path.name.toInt)
    }

    case JoinFinished => {
      joinCount += 1
      //println("Number of nodes joined: " + count)
      if (joinCount == numNodes) {
        //joinCount = 0

        //networkNodes.foreach(nodeId => nodeId ! PrintState)

        println("Network join complete for nodes. Starting Messaging")
        //networkNodes.foreach(nodeId => context.actorSelection(nodeId.toString) ! StartMessaging(numRequests))
        networkNodes.foreach(nodeId => nodeId ! StartMessaging(numRequests))

        startFailingNodes()
      } else if (joinCount < numNodes) {
        //println("Starting node number" + (count + 1))
        //context.actorSelection(networkNodes(joinCount).toString) ! StartJoin(networkNodes(joinCount - 1))
        networkNodes(joinCount) ! StartJoin(networkNodes(joinCount - 1).path.name.toInt)
      }
    }

    case MessagingComplete(numHops, numRequests) => {
      msgCount += 1
      totalHops += numHops
      totalRequests += numRequests

      if (msgCount == numNodes) {
        //println("Overall Average: " + (totalHops / (numNodes * numRequests)))
        println("Overall Average: " + (totalHops / totalRequests))
        context.system.shutdown
      }
    }

    case FailNode => {
      var rand = new Random

      if (numFailedNodes < numFailures) {
        var randNum = rand.nextInt(numNodes)
        //context.stop(networkNodes(randNum))
        networkNodes(randNum) ! Stop
        numFailedNodes += 1
      }
    }

    case Stop => {
      context.system.shutdown
    }

    case _ => {
      println("Unexpected Message !!!")
      //context.system.shutdown
    }
  }

  def startFailingNodes() {

    val system = context.system
    import system.dispatcher
    scheduler = system.scheduler.schedule(Duration(100, TimeUnit.MILLISECONDS), Duration(100, TimeUnit.MILLISECONDS), self, FailNode)
  }
}

class PastryNode(pNodeId: Int) extends Actor {
  val nodeId = pNodeId
  var isAlive = true
  var pathToClosestNode: Array[Int] = Array.emptyIntArray
  //Array to store nodeIds which this node has already sent welcome message to
  var welcomeNodeList: Array[Int] = Array.emptyIntArray
  var msgNodeList: Array[Tuple2[Int, Int]] = Array.empty
  var countUpdatedNodes = 0
  var nodeState = new PastryNodeState(nodeId)
  var scheduler: Cancellable = null
  //var randomNumList: List[Int] = null
  var numMsgs = 0
  var msgSentCount = 0
  var msgDeliveredCount = 0
  var numHopsList: Array[Int] = Array.emptyIntArray
  var msg: String = "Hi"

  //Initialize routing table
  //println("Initializing routing table of node " + nodeId)
  nodeState.initializeState(nodeId)

  def receive = {
    case ActorIdentity(_, Some(actorRef)) => {
      context watch actorRef
    }

    case ActorIdentity(_, None) => { // not alive

    }

    case Terminated(node) => { // ...
      //nodeState.removeNodeFromStateTables((node.path.name).toInt)
    }

    case StartJoin(neighbourKey) => {
      // println(nodeId + " started join with neighbour " + neighbourKey)
      context.actorSelection("../" + neighbourKey.toString) ! Join(nodeId)
    }

    case Join(key) => {
      val senderNodeId: Int = (sender.path.name).toInt
      //println(nodeId + " got join request from " + key + " via " + senderNodeId)
      if (welcomeNodeList.contains(key)) {
        context.actorSelection("../" + key.toString) ! JoinFinish(nodeState)
      } else {
        welcomeNodeList +:= key
        val numDigitsShared: Int = nodeState.sharedPrefix(key)
        context.actorSelection("../" + key.toString) ! Welcome(nodeState, numDigitsShared)

        //forward this request further
        //check for the closest node to this nodeId in your leafset/routing table and forward to that node
        val closestNodeId: Int = nodeState.nextNode(key)
        //println("nodeId closest to " + key + " from " + nodeId + " is " + closestNodeId)
        //println("("+closestNodeId+" == "+nodeId+") = "+(closestNodeId == nodeId)+" || ("+closestNodeId+" == "+senderNodeId+") = "+(closestNodeId == senderNodeId))
        if (closestNodeId == nodeId || closestNodeId == senderNodeId) {
          //this is the closest node
          //context.actorSelection("../" + key.toString) ! JoinFinish(nodeState.smallerLeafSet, nodeState.largerLeafSet)
          context.actorSelection("../" + key.toString) ! JoinFinish(nodeState)
        } else {
          context.actorSelection("../" + closestNodeId.toString) ! Join(key)
          //context.actorSelection("../" + closestNodeId.toString) ! Join(key, n + numDigitsShared)
        }
      }
    }

    case Welcome(pNodeState, pNumRowsRoutingTable) => {
      pathToClosestNode :+= (sender.path.name).toInt
      //println(nodeId + " updating its routing table rows " + pNumRowsRoutingTable + " received from " + sender.path.name)
      nodeState.updateRoutingTable(pNodeState, pNumRowsRoutingTable)
    }

    case UpdateState(pNodeState) => {
      //update leafset
      //println(nodeId + " updating its state using state received from " + sender.path.name)
      nodeState.updateState(pNodeState)
      //nodeState.printState()
      sender ! UpdateFinished
    }

    case UpdateFinished => {
      countUpdatedNodes += 1

      if (countUpdatedNodes >= pathToClosestNode.length) {
        //println("join finished for " + nodeId)
        context.parent ! JoinFinished
      }
    }

    case JoinFinish(pNodeState) => {
      //println("join finished for " + nodeId)
      //println("Number of nodes in path: " + pathToClosestNode.length)
      //println("nodesInPath: " + Arrays.toString(pathToClosestNode))

      val closestNodeId = (sender.path.name).toInt

      nodeState.updateLeafSet(pNodeState)

      //nodeState.printState()

      pathToClosestNode.foreach(nodeId => context.actorSelection("../" + nodeId.toString) ! UpdateState(nodeState))

      //set death watch for all nodes in leafset and routing table
      //setWatch()
    }

    case StartMessaging(pNumMsgs) => {
      //println(nodeId + " starting messaging")
      //releasing memory for garbage collection
      welcomeNodeList = null

      numMsgs = pNumMsgs
      numHopsList = new Array(numMsgs)
      //randomNumList = Random.shuffle(((1 until Constants.keySpace).toList)) diff List(nodeId)
      val system = context.system
      import system.dispatcher
      scheduler = system.scheduler.schedule(Duration.Zero, Duration(1000, TimeUnit.MILLISECONDS), self, Deliver(msg))
    }

    case Deliver(msg) => {
      //check self leafset and routing table and forward this msg to closest node
      if (msgSentCount < numMsgs) {
        val randomdNodeId = NodeId.randomId2()
        var closestNodeId = nodeState.nextNode(randomdNodeId)
        //println(nodeId + " started delivering msg" + msgSentCount + " to " + randomdNodeId + " via " + closestNodeId)
        if (closestNodeId == nodeId) {
          numHopsList(msgSentCount) = 0
          msgDeliveredCount += 1
          if (msgDeliveredCount == numMsgs) {
            var avgHops = avg(numHopsList)
            println("Avg. number of hops for " + nodeId + ": " + avgHops)
            context.parent ! MessagingComplete(avgHops * numMsgs, numMsgs)
          }
        } else {
          //var path: Array[Int] = Array(nodeId)
          //context.actorSelection("../" + closestNodeId.toString) ! Forward(randomdNodeId, nodeId, msg, path, msgSentCount)
          context.actorSelection("../" + closestNodeId.toString) ! Forward(randomdNodeId, nodeId, msg, msgSentCount, 0)
        }
        msgSentCount += 1
      } else {
        scheduler.cancel()
      }
    }

    //case Forward(key, senderId, msg, path, msgCount) => {
    case Forward(key, senderId, msg, msgCount, numHops) => {
      //println(nodeId + " received msg" + msgCount + " from " + sender.path.name + " for delivering to " + key)
      //println("Delivery Path for "+key+" : " + Arrays.toString(path))
      if (isAlive) {
        if (senderId == nodeId) {
          //finished
          numHopsList(msgCount) = 0
          msgDeliveredCount += 1
          if (msgDeliveredCount == numMsgs) {
            var avgHops = avg(numHopsList)
            println("Avg. number of hops for " + nodeId + ": " + avgHops)
            context.parent ! MessagingComplete(avgHops * numMsgs, numMsgs)
          }
        } else if (msgNodeList.contains((senderId, msgCount))) {
          //finished
          //println(senderId + " delivered msg" + msgCount + " to " + key + " on path: " + Arrays.toString(path))
          context.actorSelection("../" + senderId.toString) ! MessageDelivered(numHops, msgCount)
        } else {
          //var newPath: Array[Int] = path.clone
          //newPath +:= nodeId
          msgNodeList +:= (senderId, msgCount)
          var closestNodeId = nodeState.nextNode(key)
          if (closestNodeId == nodeId) {
            //finished
            //println(senderId + " delivered msg" + msgCount + " to " + key + " on path: " + Arrays.toString(newPath))
            context.actorSelection("../" + senderId.toString) ! MessageDelivered(numHops + 1, msgCount)
          } else {
            context.actorSelection("../" + closestNodeId.toString) ! Forward(key, senderId, msg, msgCount, numHops + 1)
          }
        }
      } else {
        //send it back to sender
        sender ! NodeDead(key, senderId, msg, msgCount, numHops + 1)
      }
    }

    case NodeDead(key, senderId, msg, msgCount, numHops) => {
      var senderNodeId: Int = sender.path.name.toInt
      val (askLeafSetFromNode, askRoutingTableFromNode) = nodeState.removeDeadNodeAndUpdateState(senderNodeId)

      if (askLeafSetFromNode != 0)
        context.actorSelection("../" + askLeafSetFromNode.toString) ! RequestForLeafSet

      if (askRoutingTableFromNode != 0)
        context.actorSelection("../" + askRoutingTableFromNode.toString) ! RequestForRoutingTable

      var closestNodeId = nodeState.nextNode(key)
      if (closestNodeId == nodeId) {
        //finished
        //println(senderId + " delivered msg" + msgCount + " to " + key + " on path: " + Arrays.toString(newPath))
        context.actorSelection("../" + senderId.toString) ! MessageDelivered(numHops + 1, msgCount)
      } else {
        context.actorSelection("../" + closestNodeId.toString) ! Forward(key, senderId, msg, msgCount, numHops + 1)
      }
    }

    case RequestForLeafSet => {
      sender ! UpdateLeafSet(nodeState)
    }

    case RequestForRoutingTable => {
      sender ! UpdateRoutingTable(nodeState)
    }

    case UpdateLeafSet(pNodeState) => {
      nodeState.updateLeafSet(pNodeState)
    }

    case UpdateRoutingTable(pNodeState) => {
      nodeState.updateRoutingTable(pNodeState, Constants.numRows)
    }

    //case MessageDelivered(path, msgCount) => {
    case MessageDelivered(numHops, msgCount) => {
      if (msgCount < numMsgs) {
        //numHops(msgCount) = path.length
        numHopsList(msgCount) = numHops
        //println(nodeId + " delivered msg" + msgCount + " in " + numHops(msgCount) + " hops")
        msgDeliveredCount += 1
        if (msgDeliveredCount == numMsgs) {
          var avgHops = avg(numHopsList)
          println("Avg. number of hops for " + nodeId + ": " + avgHops)
          context.parent ! MessagingComplete(avgHops * numMsgs, numMsgs)
        }
      }
    }

    case PrintState => {
      nodeState.printStateToFile()
    }

    case Stop => {
      //context.system.shutdown
      if (isAlive) {
        isAlive = false
        scheduler.cancel()
        if (msgDeliveredCount < numMsgs) {
          var avgHops = avg(numHopsList)
          println("Avg. number of hops for " + nodeId + ": " + avgHops+" ("+msgDeliveredCount+")")
          context.parent ! MessagingComplete(avgHops * msgDeliveredCount, msgDeliveredCount)
        }
      }
    }

    case _ => {
      println("Unexpected Message !!!")
      context.system.shutdown
    }
  }

  def avg(numList: Array[Int]): Double = {
    var sum: Double = 0
    numList.foreach(x => sum += x)

    return (sum / numList.length)
  }

  def setWatch() {
    nodeState.smallerLeafSet.foreach(nodeId =>
      context.actorSelection("../" + nodeId._1.toString) ! Identify(None))

    nodeState.largerLeafSet.foreach(nodeId =>
      context.actorSelection("../" + nodeId._1.toString) ! Identify(None))

    var rt = nodeState.routingTable
    for (i <- 0 until Constants.numRows) {
      for (j <- 0 until Constants.numCols) {
        var nodeId = rt(i)(j)
        if (nodeId != null) {
          context.actorSelection("../" + nodeId._1.toString) ! Identify(None)
        }
      }
    }
  }
}

class PastryNodeState(pNodeId: Int) {
  val nodeId = pNodeId
  //var smallestLeafNodeId: Int = Int.MaxValue
  var smallestLeafNodeId: Int = 0
  //var largestLeafNodeId: Int = Int.MaxValue 
  var largestLeafNodeId: Int = 0
  var smallerLeafSet, largerLeafSet: Array[Tuple2[Int, Int]] = new Array(Constants.L / 2) //array of size L/2
  var leafSet: Array[Tuple2[Int, Int]] = new Array(Constants.L) //array of size L
  var lenSmallerLeafSet, lenLargerLeafSet: Int = 0
  var routingTable = Array.ofDim[Tuple2[Int, Int]](Constants.numRows, Constants.numCols)
  var pathToClosestNode: Array[Int] = Array.emptyIntArray
  var lastUpdated = 0 //TODO 

  def initializeState(nodeId: Int) {
    //initializing routing table
    //adding nodeId in the index where nodeId shares the ith digit
    var num = nodeId
    for (i <- 1 until Constants.numRows) {
      var index = (num / Math.pow(10, Constants.numRows - i)).toInt
      num = (num % Math.pow(10, Constants.numRows - i)).toInt
      routingTable(i)(index) = (nodeId, 0)

      //println(nodeId+" : "+Arrays.toString(routingTable(i)))
    }
  }

  def removeNodeFromLeafSet(key: Int): Int = {
    val diff = Math.abs(key - nodeId)
    var existInLeafSet = false
    var askLeafSetFromNode: Int = 0

    //removing node from leafset if exists
    if (smallerLeafSet.contains((key, diff))) {
      smallerLeafSet = smallerLeafSet diff List((key, diff))
      lenSmallerLeafSet -= 1
      existInLeafSet = true
    } else if (largerLeafSet.contains((key, diff))) {
      largerLeafSet = largerLeafSet diff List((key, diff))
      lenLargerLeafSet -= 1
      existInLeafSet = true
    }

    //updating smallest & largest nodeId in leafSet
    if (existInLeafSet) {
      if (nodeId == smallestLeafNodeId) {
        if (lenSmallerLeafSet > 0) {
          smallestLeafNodeId = smallerLeafSet(0)._1
        } else if (lenLargerLeafSet > 0) {
          smallestLeafNodeId = largerLeafSet(0)._1
        } else {
          smallestLeafNodeId = 0
        }
      } else if (nodeId == largestLeafNodeId) {
        if (lenLargerLeafSet > 0) {
          largestLeafNodeId = largerLeafSet(lenLargerLeafSet - 1)._1
        } else if (lenSmallerLeafSet > 0) {
          largestLeafNodeId = smallerLeafSet(lenSmallerLeafSet - 1)._1
        } else {
          largestLeafNodeId = 0
        }
      }

      if (key < nodeId) {
        askLeafSetFromNode = smallestLeafNodeId
      } else {
        askLeafSetFromNode = largestLeafNodeId
      }
    }

    return askLeafSetFromNode
  }

  def removeNodeFromRoutingTable(key: Int): Int = {
    var askRoutingEntryFromNode: Int = 0
    //update routing table entry
    var sharedDigits = sharedPrefix(key)
    if (sharedDigits < Constants.digits) {
      //next digit in routing entry nodeId after the shared digits
      var nextDigit = (((key / Math.pow(10, (Constants.digits - 1) - sharedDigits)).toInt) % 10).toInt
      var pDiff = Math.abs(nodeId - key)

      var routingEntry = routingTable(sharedDigits)(nextDigit)
      if (routingEntry != null && routingEntry == Tuple2(key, pDiff)) {
        routingTable(sharedDigits)(nextDigit) = null

        breakable {
          for (i <- 0 until Constants.numCols) {
            if (routingTable(sharedDigits)(i) != null) {
              askRoutingEntryFromNode = routingTable(sharedDigits)(i)._1
              break
            }
          }
        }

      }
    }

    return askRoutingEntryFromNode
  }

  def updateState(pNodeState: PastryNodeState) {
    updateLeafSet(pNodeState)
    updateRoutingTable(pNodeState, Constants.numRows)
  }

  def updateLeafSet(pNodeState: PastryNodeState) {
    //Updating leafSet
    val pNodeId = pNodeState.nodeId

    //Adding all nodes of the current node to tempLeafSet
    var tempSmallerLeafSet: Array[Tuple2[Int, Int]] = (smallerLeafSet.slice(0, lenSmallerLeafSet)).clone
    var tempLargerLeafSet: Array[Tuple2[Int, Int]] = (largerLeafSet.slice(0, lenLargerLeafSet)).clone

    //Adding nodes of the neighbour node to the tempLeafSet
    for (i <- 0 until pNodeState.lenSmallerLeafSet) {
      var pLeafNodeId = pNodeState.smallerLeafSet(i)
      var diff = (Math.abs(nodeId - pLeafNodeId._1)).toInt

      if (pLeafNodeId._1 < nodeId) {
        if (!tempSmallerLeafSet.contains((pLeafNodeId._1, diff)))
          tempSmallerLeafSet :+= (pLeafNodeId._1, diff)
      } else if (pLeafNodeId._1 > nodeId) {
        if (!tempLargerLeafSet.contains((pLeafNodeId._1, diff)))
          tempLargerLeafSet :+= (pLeafNodeId._1, diff)
      }
    }
    for (i <- 0 until pNodeState.lenLargerLeafSet) {
      var pLeafNodeId = pNodeState.largerLeafSet(i)
      var diff = (Math.abs(nodeId - pLeafNodeId._1)).toInt
      if (pLeafNodeId._1 < nodeId) {
        if (!tempSmallerLeafSet.contains((pLeafNodeId._1, diff)))
          tempSmallerLeafSet :+= (pLeafNodeId._1, diff)
      } else if (pLeafNodeId._1 > nodeId) {
        if (!tempLargerLeafSet.contains((pLeafNodeId._1, diff)))
          tempLargerLeafSet :+= (pLeafNodeId._1, diff)
      }
    }
    var diff = (Math.abs(nodeId - pNodeId).toInt)

    if (pNodeId < nodeId) {
      if (!tempSmallerLeafSet.contains((pNodeId, diff)))
        tempSmallerLeafSet :+= (pNodeId, diff)
    } else {
      if (!tempLargerLeafSet.contains((pNodeId, diff)))
        tempLargerLeafSet :+= (pNodeId, diff)
    }

    //Sorting tempLeafSet based on difference from nodeId
    tempSmallerLeafSet.sortBy(_._2)
    tempLargerLeafSet.sortBy(_._2)

    //Selecting first L/2 elements from each tempLeafSet
    if (tempSmallerLeafSet.length > Constants.L / 2) {
      smallerLeafSet = tempSmallerLeafSet.slice(0, Constants.L / 2).clone
      lenSmallerLeafSet = Constants.L / 2
    } else {
      smallerLeafSet = tempSmallerLeafSet.clone
      lenSmallerLeafSet = smallerLeafSet.length
    }
    if (tempLargerLeafSet.length > Constants.L / 2) {
      largerLeafSet = tempLargerLeafSet.slice(0, Constants.L / 2).clone
      lenLargerLeafSet = Constants.L / 2
    } else {
      largerLeafSet = tempLargerLeafSet.clone
      lenLargerLeafSet = largerLeafSet.length
    }

    //setting the largest and smaller nodeId in the leafset (required for routing)
    if (tempSmallerLeafSet.length > 0) {
      smallestLeafNodeId = smallerLeafSet(0)._1
    } else if (tempLargerLeafSet.length > 0) {
      smallestLeafNodeId = largerLeafSet(0)._1
    }
    if (tempLargerLeafSet.length > 0) {
      largestLeafNodeId = largerLeafSet(lenLargerLeafSet - 1)._1
    } else if (tempSmallerLeafSet.length > 0) {
      largestLeafNodeId = smallerLeafSet(lenSmallerLeafSet - 1)._1
    }
  }

  def updateRoutingTable(pNodeState: PastryNodeState, numRows: Int) {
    //Updating routingTable
    val pRoutingTable = pNodeState.routingTable
    for (i <- 0 until numRows) {
      for (j <- 0 until Constants.numCols) {
        var pRoutingEntry = pRoutingTable(i)(j)
        if (pRoutingEntry != null) {
          checkAndUpdateRoutingEntry(pRoutingEntry._1)
          /*var sharedDigits = sharedPrefix(pRoutingEntry._1)
          if (sharedDigits < Constants.digits) {
            //next digit in routing entry nodeId after the shared digits
            var nextDigit = (((pRoutingEntry._1 / Math.pow(10, (Constants.digits - 1) - sharedDigits)).toInt) % 10).toInt
            var pDiff = Math.abs(nodeId - pRoutingEntry._1)

            var routingEntry = routingTable(sharedDigits)(nextDigit)
            if (routingEntry != null) {
              if (pDiff < routingEntry._2) { //if routing entry of neighbour node is closer than current routing entry then replace it with neighbour routing entry
                routingTable(sharedDigits)(nextDigit) = (pRoutingEntry._1, pDiff)
              }
            } else {
              routingTable(sharedDigits)(nextDigit) = (pRoutingEntry._1, pDiff)
            }
          }*/
        }
      }
    }
    checkAndUpdateRoutingEntry(pNodeState.nodeId)

    def checkAndUpdateRoutingEntry(pRoutingEntryNodeId: Int) {
      var sharedDigits = sharedPrefix(pRoutingEntryNodeId)
      if (sharedDigits < Constants.digits) {
        //next digit in routing entry nodeId after the shared digits
        var nextDigit = (((pRoutingEntryNodeId / Math.pow(10, (Constants.digits - 1) - sharedDigits)).toInt) % 10).toInt
        var pDiff = Math.abs(nodeId - pRoutingEntryNodeId)

        var routingEntry = routingTable(sharedDigits)(nextDigit)
        if (routingEntry != null) {
          if (pDiff < routingEntry._2) { //if routing entry of neighbour node is closer than current routing entry then replace it with neighbour routing entry
            routingTable(sharedDigits)(nextDigit) = (pRoutingEntryNodeId, pDiff)
          }
        } else {
          routingTable(sharedDigits)(nextDigit) = (pRoutingEntryNodeId, pDiff)
        }
      }
    }
  }

  def nextNode(key: Int): Int = {
    var minKey = 0
    var minDiff = Int.MaxValue

    if (key >= smallestLeafNodeId && key <= largestLeafNodeId) {
      for (i <- 0 until lenSmallerLeafSet) {
        var diff = smallerLeafSet(i)._2
        if (diff < minDiff) {
          minKey = smallerLeafSet(i)._1
          minDiff = diff
        }
      }

      for (i <- 0 until lenLargerLeafSet) {
        var diff = largerLeafSet(i)._2
        if (diff < minDiff) {
          minKey = largerLeafSet(i)._1
          minDiff = diff
        }
      }
    }

    if (minKey == 0) {
      //closest key not found in leafset, search routing table
      val numDigitsInSharedPrefix = sharedPrefix(key)
      //println("Number of digits "+nodeId+" shares with "+key+": "+numDigitsInSharedPrefix)

      var rowIndex = -1

      if (numDigitsInSharedPrefix > 0) {
        rowIndex = numDigitsInSharedPrefix
      } else {
        rowIndex = 0
      }

      if (rowIndex >= 0 && rowIndex < Constants.numRows) {
        var minDiff = Integer.MAX_VALUE
        for (i <- 0 until Constants.numCols) {
          var routingTableEntry = routingTable(rowIndex)(i)
          //println(i+" routing table entry: "+routingTableEntry)
          if (routingTableEntry != null) {
            var diff = Math.abs(key - routingTableEntry._1)
            //println(i+" "+diff+" "+minDiff)
            if (diff < minDiff) {
              minKey = routingTableEntry._1
              minDiff = diff
            }
          }
        }
      }
    }

    //if no closest node is found then current node is the closest node
    if (minKey == 0) {
      minKey = nodeId
    }

    return minKey
  }

  def removeDeadNodeAndUpdateState(key: Int): Tuple2[Int, Int] = {
    //removing node from leafset if exists
    val askLeafSetFromNode = removeNodeFromLeafSet(key)
    //removing node from routing table if exists
    val askRoutingEntryFromNode = removeNodeFromRoutingTable(key)
    //println(nodeId+" state after removing "+key)
    //printState()
    return (askLeafSetFromNode, askRoutingEntryFromNode)
  }

  //this function will return number of keys shared between 2 keys
  def sharedPrefix(key: Int): Int = {
    var shared = 0
    breakable {
      for (i <- 7 to 0 by -1) {
        var q1 = (nodeId / Math.pow(10, i)).toInt
        var q2 = (key / Math.pow(10, i)).toInt

        if (q1 != q2) {
          break
        }
        shared = shared + 1
      }
    }
    return shared
  }

  def printState() {
    print("Node " + nodeId + " smallerLeafset: ")
    smallerLeafSet.foreach(nodeId => if (nodeId != null) print("(" + nodeId._1 + ", " + nodeId._2 + ")")
    else print("null"))

    println()
    print("Node " + nodeId + " largerLeafset: ")
    largerLeafSet.foreach(nodeId => if (nodeId != null) print("(" + nodeId._1 + ", " + nodeId._2 + ")")
    else print("null"))

    println()
    println("Node " + nodeId + " routing table: ")
    for (i <- 0 until Constants.numRows) {
      routingTable(i).foreach(nodeId => if (nodeId != null) print("(" + nodeId._1 + ", " + nodeId._2 + ")")
      else print("null"))
      println()
    }
  }

  def printStateToFile() {
    var pw: PrintWriter = null

    try {
      pw = new PrintWriter(new File("node_states//" + nodeId + ".txt"))

      pw.print("Node " + nodeId + " smallerLeafset: ")
      smallerLeafSet.foreach(nodeId => if (nodeId != null) pw.print("(" + nodeId._1 + ", " + nodeId._2 + ") ")
      else pw.print("null "))

      pw.println()
      pw.print("Node " + nodeId + " largerLeafset: ")
      largerLeafSet.foreach(nodeId => if (nodeId != null) pw.print("(" + nodeId._1 + ", " + nodeId._2 + ") ")
      else pw.print("null "))

      pw.println()
      pw.println("Node " + nodeId + " routing table: ")
      for (i <- 0 until Constants.numRows) {
        routingTable(i).foreach(nodeId => if (nodeId != null) pw.print("(" + nodeId._1 + ", " + nodeId._2 + ") ")
        else pw.print("null "))
        pw.println()
      }

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      try {
        if (pw != null)
          pw.close()
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }
    }
  }
}

object Constants {
  val keySize = 16 //16-bit NodeId
  val b = 2 //configuration parameter
  val L = 8 //configuration parameter
  val keySpace: Int = (1 << Constants.keySize) //(2^16)
  val numRows = Constants.keySize / Constants.b
  val numCols = (1 << Constants.b)
  val digits = Constants.keySize / Constants.b
}

object NodeId {
  //key values will be from 1 to (2^16 - 1)
  var randomNumList: List[Int] = Random.shuffle((1 until Constants.keySpace).toList)
  //println("First element in random list: " + randomNumList(0))
  var randomNumList2: List[Int] = (1 until Constants.keySpace).toList

  def randomId(): Int = {
    val randNum = randomNumList.head
    randomNumList = randomNumList diff List(randNum)

    return toBaseN(randNum)
  }

  def randomId2(): Int = {
    var rand = new Random
    var randNum = rand.nextInt(Constants.keySpace - 1)

    return toBaseN(randomNumList2(randNum))
  }

  def toBaseN(num: Int): Int = {
    val baseN = (1 << Constants.b) //2^b
    val digits = Constants.digits

    var numInBase10 = num //number in base10 that we can converting to baseN
    var numInBaseN: Int = 0 //number in baseN

    var i: Int = digits - 1
    var j: Int = 0
    while (numInBase10 >= baseN) {
      numInBaseN += ((numInBase10 % baseN) * Math.pow(10, j)).toInt //generating the digits of number in baseN starting from the least significant digit
      numInBase10 = numInBase10 / baseN
      i -= 1
      j += 1
    }

    numInBaseN += ((numInBase10) * Math.pow(10, j)).toInt

    return numInBaseN
  }
}