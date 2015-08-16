package edu.berkeley.simple_project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	public static void main(String[] args){
		String logFile = "~/Downloads/spark-1.4.0-bin-hadoop2.6/README.md";
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();
		
		long numAs = logData.filter(new Function<String, Boolean>(){
			public Boolean call(String s){return s.contains("a");}
		}).count();
		
		long numBs = logData.filter(new Function<String, Boolean>(){
			public Boolean call(String s){return s.contains("b");}
		}).count();
		
		long numCs = logData.filter(new Function<String, Boolean>(){
			public Boolean call(String s){return s.contains("c");}
		}).count();
				
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}
}
