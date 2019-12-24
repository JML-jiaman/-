import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._

/*
eventTime+滑动窗口
 */
object StreamEventTimeApp1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置 FlinkTime为eventTime,因为默认是processingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataStream: DataStream[String] = env.socketTextStream("hadoop102",7777)
    val mapDataStream: DataStream[(String, Long, Int)] = dataStream.map(data => {
      val arr: Array[String] = data.split(" ")
      (arr(0), arr(1).toLong, 1)
    })

    //告知flink那个是时间戳
    //延迟3秒
    val timeDsream: DataStream[(String, Long, Int)] = mapDataStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(3000)) {
        override def extractTimestamp(t: (String, Long, Int)): Long = {
          t._2
        }
      })

    val keyStream: KeyedStream[(String, Long, Int), Tuple] = timeDsream.keyBy(0)
    keyStream.print("text")


    //每5秒开一个窗口统计KEY的个数，5秒以一个数据的时间戳为
    //滑动大小为5秒,步长为1秒==>步长决定窗口的触发,每1秒触发一次窗口
    //滑动大小是边界值,在每1秒触动窗口时,记录这一阶段的所有值,当触发边界值时,返回此个边界值的所有数据
    val windowBykey: WindowedStream[(String, Long, Int), Tuple, TimeWindow] =
      keyStream.window(SlidingEventTimeWindows.of(Time.milliseconds(5000),Time.milliseconds(1000)))

    val sumDstream = windowBykey.sum(2)
    sumDstream.map(_._3).print("window::").setParallelism(1)

    env.execute()

  }

}
