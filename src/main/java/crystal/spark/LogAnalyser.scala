/**
  * Created by cloudera on 10/22/16.
  */
package crystal.spark

import java.util.regex.Pattern

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext


object LogAnalyser extends Serializable{
  def main(args: Array[String]) {


    val sc = new SparkContext()

    case class Record(method: String, url: String, code: String)


    val input = sc.textFile("input", 5).cache()
    def parseLine(line:String):Option[Record] = {
      val codeAndUri = Pattern.compile("\"([A-Za-z]+) (.*?) [a-zA-Z0-9./]+\" (\\d{3})").matcher(line)
      if(codeAndUri.find){
        Some(Record(codeAndUri.group(1), codeAndUri.group(2), codeAndUri.group(3)))
      } else {
        None
      }
    }



    val results = input.flatMap(line=>parseLine(line))
                        .filter(line => !(line.code.equals("") || line.url.equals("") || line.method.equals("")))
                        .map(word => ((word.url, word.method), 1))
                        .reduceByKey(_ + _)
                        .filter(x=>x._2 > 1)
                        .cache()

      val resultsForHBase = results.map(line=>{
                          val put = new Put(Bytes.toBytes(line._1._1))
                          put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("method"), Bytes.toBytes(line._1._2))
                          put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("count"), Bytes.toBytes(line._2))
                          (new ImmutableBytesWritable, put)
                        })

    val resultsForTextOutput = results.sortBy(f=>(f._1._1, f._1._2)).map(x=>x._1._2 + "\t" + x._2 + "\t" + x._1._1)



    val conf = HBaseConfiguration.create()

    conf.set("hbase.rootdir", "/hbase")
    conf.setBoolean("hbase.cluster.distributed", true)
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.setInt("hbase.client.scanner.caching", 10000)

    val jobConfig = new JobConf(conf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "AccessLog")

    resultsForHBase.saveAsHadoopDataset(jobConfig)

    resultsForTextOutput.saveAsTextFile("output")
  }
}