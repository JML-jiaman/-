import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/*
TODO:eventTime+滚动时间窗口
 */
object StreamEventTimeaApp {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //TODO: ERROR:并行度必须是1
    env.setParallelism(1)


    val datas: DataStream[String] = env.socketTextStream("hadoop102",7777)

    val value: DataStream[(String, Long,Int)] = datas.map(data => {
      val arr: Array[String] = data.split(" ")
      (arr(0), arr(1).toLong, 1)
    })

    //告知flink那个是时间戳
    val windowDstream = value.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(3000)) {
      override def extractTimestamp(t: (String, Long, Int)): Long = {
        t._2
      }
    })

    val text: KeyedStream[(String, Long, Int), Tuple] = windowDstream.keyBy(0)
    text.print("text").setParallelism(1)

    //每5秒开一个窗口统计KEY的个数，5秒以一个数据的时间戳为

   val windowByKey = text.window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
    val sumDstream = windowByKey.sum(2)
    sumDstream.map(_._3).print("window::").setParallelism(1)

    env.execute()
  }

}
