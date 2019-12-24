import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkStreamWcApp {

  def main(args: Array[String]): Unit = {
    //环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //读取
    val ds: DataStream[String] = env.socketTextStream("hadoop102",7777)
    import org.apache.flink.api.scala._
    //转换
    val result: DataStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
   //打印，设置并行度
    result.print().setParallelism(1);

    //Time 开窗函数,滚动窗口
//    val v1: KeyedStream[(String, Int), Tuple] = ds.flatMap(_.split(" ")).map((_,1)).keyBy(0)
//    val value = v1.timeWindow(Time.seconds(10))
//    val res = value.sum(1)
    //Time 开窗函数，滑动窗口
//val v1: KeyedStream[(String, Int), Tuple] = ds.flatMap(_.split(" ")).map((_,1)).keyBy(0)
//   val value = v1.timeWindow(Time.seconds(10),Time.seconds(5))
//    val res = value.sum(1)

    //Count 开窗函数
//    val v1: KeyedStream[(String, Int), Tuple] = ds.flatMap(_.split(" ")).map((_,1)).keyBy(0)
//    v1.countWindow()

    //执行
    env.execute()

  }
}
