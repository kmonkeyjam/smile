package net.lag.smile

trait ConnectionWrapper {
  def shutdown()
  def isEjected: Boolean
  def eject()
  def clearFailures()
  def registerFailure()
  def hostname: String
  def weight: Int
  def port: Int
  def connected: Boolean
  def getConnection: MemcacheConnection
  def releaseConnection(node: MemcacheConnection)
}