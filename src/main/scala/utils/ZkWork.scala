package utils

import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper._

  /**
   *
   * @author wangjx
   * zookeeper基础实现api
   */

object ZkWork {

    val TIME_OUT = 100000
    var ZOOKEEPER : ZooKeeper = _
//    var countDownLatch:CountDownLatch = new CountDownLatch(1)
//    def watcher=new Watcher {
//
//      override def process(watchedEvent: WatchedEvent): Unit = {
//        //println(s" [zkWork] process : "+watchedEvent.getType)
//        if(KeeperState.SyncConnected == watchedEvent.getState()){
//          countDownLatch.countDown()
//        }
//      }
//    }

    class ConnectedWatcher(var countDownLatch: CountDownLatch) extends Watcher {
      override def process(event: WatchedEvent): Unit = {
        if (event.getState == KeeperState.SyncConnected) {
          countDownLatch.countDown()
        }
      }
    }

    def waitUntilConnected(zooKeeper:ZooKeeper, connectedLatch:CountDownLatch) {
      if (States.CONNECTING == zooKeeper.getState()) {
        connectedLatch.await();
      }
    }
    /**
     *   基础方法
     *   连接zookerper 创建znode,更新znode
     */

    /*获取连接*/
    def connect(): Unit ={
      var countDownLatch = new CountDownLatch(1)
      var watcher: ConnectedWatcher = new ConnectedWatcher(countDownLatch)
      //println(s"[zkWork] | zk connect")
      ZOOKEEPER = new ZooKeeper("localhost:2181",TIME_OUT,watcher)
      waitUntilConnected(ZOOKEEPER,countDownLatch)
    }

    /*创建节点*/
    def znodeCreate(znode:String,data:String){
      //println(s"[zkWork] zk create /$znode,$data")
      ZOOKEEPER.create(s"/$znode",data.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT)
    }

    /*添加节点数据*/
    def znodeDataSet(znode:String,data:String): Unit ={
      //println(s"[zkWork] zk data set /$znode")
      ZOOKEEPER.setData(s"/$znode",data.getBytes(),-1)
    }


  /**
    * 获得znode数据
    * 判断znode是否存在
    * 更新znode数据
    */

    def znodeDataGet(znode:String):Array[String] = {
      connect()
      //println(s"[zkWork] zk data get /$znode")
      try {
        new String(ZOOKEEPER.getData(s"/$znode",true,null),"utf-8").split(",")
      }
      catch {
        case _:Exception =>Array()
      }
    }


    def znodeIsExists(znode:String):Boolean ={
      connect()
      //println(s"[zkWork] zk znode is exists /$znode")
      val stat = ZOOKEEPER.exists(s"/$znode", true)
      stat match {
          case null => false
          case _ => true
      }
    }

    def offsetWork(znode:String,data:String): Unit ={
      connect()
      //println(s"[zkWork] offset work /$znode")
      val stat = ZOOKEEPER.exists(s"/$znode",true)
      stat match {
          case null => znodeCreate(znode,data)
          case _ =>znodeDataSet(znode,data)
      }
      //println(s"[zkWork] zk close")
      ZOOKEEPER.close()
    }
}
