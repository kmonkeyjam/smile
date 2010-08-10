package net.lag.smile

import com.twitter.util.{FactoryPool, Future}
import collection.mutable.ListBuffer

class ConnectionPool(numItems: Int, val desc: String, val pool: ServerPool) extends FactoryPool[MemcacheConnection](numItems) {
  var connectionConfig: MemcacheConfig = null
  /**
   * Make a new MemcacheConnection out of a description string. A description string is:
   * <hostname> [ ":" <port> [ " " <weight> ]]
   * The default port is 11211 and the default weight is 1.
   */
  def makeConnection(desc: String) = {
    connectionConfig = desc.split("[: ]").toList match {
      case hostname :: Nil =>
        new MemcacheConfig(hostname)
      case hostname :: port :: Nil =>
        new MemcacheConfig(hostname, port.toInt)
      case hostname :: port :: weight :: Nil =>
        new MemcacheConfig(hostname, port.toInt, weight.toInt)
      case _ =>
        throw new IllegalArgumentException
    }

    val connection = new MemcacheConnection(connectionConfig)
    connection.pool = pool
    connection.connectionPool = this
    connection
  }

  def hostname: String = {
    connectionConfig.hostname
  }

  def weight: Int = {
    connectionConfig.weight
  }

  def port: Int = {
    connectionConfig.port
  }

  def shutdown {
    // items.foreach ((conn: Future[MemcacheConnection]) => conn().)
  }

  def isServerDown = {
    false
  }

  def makeItem(): Future[MemcacheConnection] = {
    val conn = makeConnection(desc)
    Future.constant(conn)
  }

  def isHealthy(connection: MemcacheConnection) = {
    // I think this isHealthy check is going to be redundant with the MemcacheClient logic to
    // reject ejected connections.  Not sure which model is better.
    true
  }
}
