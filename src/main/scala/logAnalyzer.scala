import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


object logAnalyzer {

def main(args: Array[String]): Unit = {
	  Logger.getLogger("org").setLevel(Level.ERROR);
  	val conf = new SparkConf().setAppName("Log Analyzer");
  	val sc = new SparkContext(conf);
  	val sqlContext = new org.apache.spark.sql.SQLContext(sc);
  	import sqlContext.implicits._

    val logRegex = """^(\S+) - - \[(\S+):(\S+):(\S+):(\S+) -\S+] "(\S+) (\S+) (\S+)\/\S+ (\S+) (\S+)""".r; // Regex pattern to parse the log
  	val logFile = sc.textFile("../hacks/spark/feelers/loganalyzer/access_log"); // change the location of the file.

    // LogFile RDD is filtered ( if a line doesn't conform to the regex pattern ) and converted to dataframe.
  	val logs = logFile.filter(line=>line.matches(logRegex.toString)).map(line=>parseLog(line,logRegex)).toDF();
    
    // Maximum size.
    logs.groupBy().max("size").show();
    // Minimum Size
    logs.groupBy().min("size").show();
    // average Size
    logs.groupBy().avg("size").show();

    // Count of response codes.
    logs.select("response").groupBy("response").count().show();

    //All IPAddresses that have accessed this server more than N times , Where N = 10.
    logs.select("ip").groupBy("ip").count().where("count > 10").show()

    // The top endpoints requested by count.
    logs.select("uri").groupBy("uri").count().orderBy($"count".desc).limit(10).show();

}

case class log(ip:String,date:String,hour:Int,min:Int,sec:Int,methodType:String,uri:String,protocol:String,response:Int,size:Int);

// Parses log and returns case class of the returned values.
def parseLog(line: String,logRegex:scala.util.matching.Regex):log = {
	val logRegex(ip,date,hour,min,sec,methodType,uri,protocol,response,size) = line;
	log(ip,date,hour.toInt,min.toInt,sec.toInt,methodType,uri,protocol,response.toInt,assertInt(size));
}

// Function to assert if a String has integer and if not return 0 by default.
def assertInt(variable:String):Int = {
    if(variable.forall(_.isDigit))
        return variable.toInt
    else
        return 0;
}


	
}