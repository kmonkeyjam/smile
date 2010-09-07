package net.lag.smile

import com.twitter.util.{Future, FactoryPool}

class MemcacheConfig(var hostname: String, var port: Int, var weight: Int) {
  def this(hostname: String, port: Int) {
    this(hostname, port, 1)
  }

  def this(hostname: String) {
    this(hostname, 11211)
  }
}

class ConnectionPool(val hostname: String, val port: Int, val weight: Int, numItems: Int, val pool: ServerPool) extends FactoryPool[Throwable, MemcacheConnection](numItems) with ConnectionWrapper {
  def shutdown {
    // items.foreach ((conn: Future[MemcacheConnection]) => conn().)
  }

  def getConnection = {
    reserve()()
  }

  def isEjected = false
  
  def releaseConnection(node: MemcacheConnection) {
    release(node)
  }

  def makeItem(): Future[Throwable, MemcacheConnection] = {
    val connection = new MemcacheConnection(hostname, port, weight)
    connection.pool = pool
    Future(connection)
  }

  override def toString() = {
    "<MemcacheConnection %s:%d weight=%d (%s)>".format(hostname, port, weight, "connected")
  }

  def isHealthy(connection: MemcacheConnection) = {
    // I think this isHealthy check is going to be redundant with the MemcacheClient logic to
    // reject ejected connections.  Not sure which model is better.
    true
  }
}
