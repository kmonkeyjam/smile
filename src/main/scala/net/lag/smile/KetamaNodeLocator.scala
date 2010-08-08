/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag.smile

import net.lag.extensions._
import scala.collection.jcl
import java.nio.{ByteBuffer, ByteOrder}
import java.security.MessageDigest


class KetamaNodeLocator(hasher: KeyHasher) extends NodeLocator {
  private val NUM_REPS = 160

  private var pool: ServerPool = null
  private val continuum = new jcl.TreeMap[Long, ConnectionPool]


  def this() = this(KeyHasher.KETAMA)

  def setPool(pool: ServerPool) = synchronized {
    this.pool = pool
    createContinuum
  }

  def findNode(key: Array[Byte]): MemcacheConnection = synchronized {
    val hash = hasher.hashKey(key)
    val tail = continuum.underlying.tailMap(hash)
    continuum(if (tail.isEmpty) continuum.firstKey else tail.firstKey).reserve()()
  }

  private def computeHash(key: String, alignment: Int) = {
    val hasher = MessageDigest.getInstance("MD5")
    hasher.update(key.getBytes("utf-8"))
    val buffer = ByteBuffer.wrap(hasher.digest)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.position(alignment << 2)
    buffer.getInt.toLong & 0xffffffffL
  }

  private def createContinuum() = {
    // we use (NUM_REPS * #servers) total points, but allocate them based on server weights.
    val serverCount = pool.liveServers.size
    val totalWeight = pool.liveServers.foldLeft(0.0) { _ + _.weight }
    continuum.clear

    for (connectionPool <- pool.liveServers) {
      val percent = connectionPool.weight.toDouble / totalWeight
      // the tiny fudge fraction is added to counteract float errors.
      val itemWeight = (percent * serverCount * (NUM_REPS / 4) + 0.0000000001).toInt
      for (k <- 0 until itemWeight) {
        val key = if (connectionPool.port == 11211) {
          connectionPool.hostname + "-" + k
        } else {
          connectionPool.hostname + ":" + connectionPool.port + "-" + k
        }
        for (i <- 0 until 4) {
          continuum += (computeHash(key, i) -> connectionPool)
        }
      }
    }

    assert(continuum.size <= NUM_REPS * serverCount)
    assert(continuum.size >= NUM_REPS * (serverCount - 1))
  }

  override def toString() = {
    "<KetamaNodeLocator hash=%s nodes=%d servers=%d>".format(hasher, continuum.size,
      pool.connectionPools.size)
  }
}
