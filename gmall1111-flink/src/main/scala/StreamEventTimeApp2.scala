import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/*
eventTime+会话窗口
 */
object StreamEventTimeApp2{
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
    //空隙时间为5秒基础上延迟3秒=8秒是触发窗口
    //abc-1000(毫秒),abc-2000,abc-7001(触发窗口但是有延迟),abc-10000(触发)==>窗口大小为2(5000以内的数据)
    val timeDsream: DataStream[(String, Long, Int)] = mapDataStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(3000)) {
        override def extractTimestamp(t: (String, Long, Int)): Long = {
          t._2
        }
      })

    val keyStream: KeyedStream[(String, Long, Int), Tuple] = timeDsream.keyBy(0)
    keyStream.print("text")


    //会话窗口，空隙时间为5秒,触发窗口是两个数据的差值为5的时候触发窗口,但是有延迟,只记录这个时间的数据
     val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] =
       keyStream.window(EventTimeSessionWindows.withGap(Time.milliseconds(5000)))
    env.execute()

  }

}
