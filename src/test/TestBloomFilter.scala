import com.util.{RedisUtil, jdbcConnectUtil}

object TestBloomFilter {
  def main(args: Array[String]): Unit = {
    val jedis = RedisUtil.getConnection()
    val name = jedis.get("name")
    println(name)
    println(1)
    if (name == null || name.equals("")) {
      val connection = jdbcConnectUtil.getConnect()
      val statement = connection.createStatement()
      val sql = "select name from pc where id= 1"
      val res = statement.executeQuery(sql)
      var name: String = null
      if (res.next()) {
        name = res.getString("name")
        println(name)
      }
      connection.close()
    }
    jedis.close()
  }

}
