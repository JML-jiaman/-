import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object FlinkBathWcApp {
  def main(args: Array[String]): Unit = {
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val input = tool.get("input")
    val output = tool.get("output")
    //TODO 环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //TODO 数据源
      val text: DataSet[String] = environment.readTextFile(input)
    import org.apache.flink.api.scala._
    //TODO transform转换
    val wc: AggregateDataSet[(String, Int)] = text.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    //TODO sink



  }


}
