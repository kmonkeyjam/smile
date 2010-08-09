package net.lag.smile

import com.twitter.util.{FactoryPool, Future}

class FakeConnectionPool(numConn: Int, val connection: FakeMemcacheConnection) extends FactoryPool[FakeMemcacheConnection](numConn) {
  def makeItem(): Future[FakeMemcacheConnection] = {
    Future.constant(connection)
  }

  def isHealthy(connection: FakeMemcacheConnection) = {
    true
  }
}