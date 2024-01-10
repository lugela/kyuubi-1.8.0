package org.apache.kyuubi.plugin.lineage.dispatcher.neo4j

import org.apache.hadoop.util.ShutdownHookManager
import org.apache.kyuubi.plugin.lineage.dispatcher.neo4j.Neo4jClientConf.{CLIENT_PASSWORD, CLIENT_USERNAME, NEO4J_URI}
import org.apache.spark.internal.Logging
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session}

trait Neo4jClient extends AutoCloseable {
  def getSession(): Session
}


class Neo4jRestClient(conf: Neo4jClientConf) extends Neo4jClient with Logging{
  logInfo("我要开始创建资源。。。。。。")
  private val driver: Driver = GraphDatabase.driver(conf.get(NEO4J_URI), AuthTokens.basic(conf.get(CLIENT_USERNAME), conf.get(CLIENT_PASSWORD)))
  val session = driver.session


  override def getSession(): Session = {
    logInfo("我要开始使用资源。。。。。。")
    session
  }

  override def close(): Unit = {
    logInfo("我要开始关闭资源了。。。。。。")
    session.close()
    driver.close()
  }
}



object Neo4jClient{

  @volatile private var client: Neo4jClient = _

  def getClient(): Neo4jClient = {
    if (client == null) {
      Neo4jClient.synchronized {
        if (client == null) {
          val clientConf = Neo4jClientConf.getConf()
          client = new Neo4jRestClient(clientConf)
          registerCleanupShutdownHook(client)
        }
      }
    }
    client
  }

  private def registerCleanupShutdownHook(client: Neo4jClient): Unit = {
    ShutdownHookManager.get.addShutdownHook(
      () => {
        if (client != null) {
          client.close()
        }
      },
      Integer.MAX_VALUE)
  }
}
