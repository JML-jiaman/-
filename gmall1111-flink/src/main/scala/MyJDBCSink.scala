//import java.sql
//import java.sql.{Connection, PreparedStatement}
//import java.util.Properties
//
//import com.alibaba.druid.pool.DruidDataSourceFactory
//import javax.sql.DataSource
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
//
//object MyJDBCSink {
//  def main(args: Array[String]): Unit = {
//    //环境
//     var dstream: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//  }
//
//}
//class MyJDBC(sql:String) extends RichSinkFunction[Array[Any]] {
//  var connection: Connection = null
//
//  //创建连接
//  override def open(parameters: Configuration): Unit = {
//    val properties = new Properties();
//    properties.put("driver", properties.getProperty("driver"))
//    properties.put("url", properties.get("url"))
//    properties.put("username", properties.get("username"))
//    properties.put("password", properties.get("password"))
//
//    val source: DataSource = DruidDataSourceFactory.createDataSource(properties)
//    connection = source.getConnection()
//  }
//
//
//
//
//  //反复调用
//  override def invoke(value: Array[Any]): Unit = {
//    val statement: PreparedStatement = connection.prepareStatement(sql)
//    println(value.mkString(","))
//    for (i <- 0 until value.length) {
//      statement.setObject(i + 1, value(i))
//    }
//    statement.executeUpdate()
//
//  }
//
//  override def close(): Unit = {
//    if(connection !=null){
//      connection.close()
//    }
//  }
//  }
//
