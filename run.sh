#!/usr/bin/env bash
if [ ! -f "input/input" ]; then
	echo "input file is required.";
	return;
fi

rm -r -f output;
mkdir "output";

hadoop fs -rm -r -f /user/cloudera/input
hadoop fs -rm -r -f /user/cloudera/output

hadoop fs -put input/input /user/cloudera
mvn package -DskipTests
echo "SUBMITTING JAR FILE..."
spark-submit --class crystal.spark.LogAnalyser target/crystal-spark-1.0-SNAPSHOT.jar
echo "Copying output to local"
hadoop fs -get /user/cloudera/output/part-00000 output
hadoop fs -get /user/cloudera/output/part-00001 output
hadoop fs -get /user/cloudera/output/part-00002 output
hadoop fs -get /user/cloudera/output/part-00003 output
hadoop fs -get /user/cloudera/output/part-00004 output
echo "Runnning HBaseAnalyser"
spark-submit --class crystal.spark.HBaseAnalyser target/crystal-spark-1.0-SNAPSHOT.jar
