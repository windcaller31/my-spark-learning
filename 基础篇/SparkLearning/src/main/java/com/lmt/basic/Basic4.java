package com.lmt.basic;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class Basic4 {
	public static void main(String args[]) {
		SparkConf conf = new SparkConf().setAppName("basic1").setMaster("local[*]");
		Configuration config = new Configuration();
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd1 = sc.textFile("/Users/iyx/Documents/workspace/SparkLearning/TestFile/test.txt");
		
		JavaRDD<String> basicRdd = rdd1.flatMap(new FlatMapFunction<String,String>(){
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}			
		});
		
		//join的方法 把 rdd1 和 rdd2 合并
		JavaPairRDD<String,Integer> rdd2 = basicRdd.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t,1);
			}
		});
		
		
		JavaPairRDD<String,Integer> rdd3 = basicRdd.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t,10);
			}
		});
		
		JavaPairRDD<String,Tuple2<Integer,Integer>> pairRdd = rdd2.join(rdd3);
		pairRdd.foreach(f -> {
			System.out.println(f._1 + " : " + f._2._1 + "---" + f._2._2); 
		} );
		
		//去掉join 利用 broadcast
		JavaPairRDD<String,Integer> rdd2Data = basicRdd.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t,1);
			}
		});
		Broadcast<JavaPairRDD< String, Integer >> rdd2DataBroadcast = sc.broadcast(rdd2Data);
		JavaPairRDD<String,Tuple2<Integer,Integer>> pairRddData = rdd3.join(rdd2DataBroadcast.getValue());
		pairRddData.foreach(f -> {
			System.out.println(f._1 + " >>> " + f._2._1 + "-++-" + f._2._2); 
		} );
	}
}
