package net.lag.smile

trait ConnectionWrapper {
  def shutdown()
  def hostname: String
  def weight: Int
  def port: Int
  def getConnection: MemcacheConnection
  def isEjected: Boolean 
  def releaseConnection(node: MemcacheConnection)
}