package com.lmt.basic;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Basic5 {
	public static void main(String args[]) {
		SparkConf conf = new SparkConf().setAppName("basic1").setMaster("local[*]");
		Configuration config = new Configuration();
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd1 = sc.textFile("/Users/iyx/Documents/workspace/SparkLearning/TestFile/test.txt");
		JavaPairRDD<String,Integer> basicRDD = rdd1.mapToPair(new PairFunction<String,String,Integer>(){
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String,Integer>(t,1);
			}			
		});
		
		//partition 一次处理一批数据 而非partition方法一次处理一个tuple
		
		// map partition
		JavaRDD<String> rdd2 = basicRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Integer>>,String>(){
			public Iterator<String> call(Iterator<Tuple2<String, Integer>> t) throws Exception {
				return Arrays.asList(t.next()._1+"==+++=="+t.next()._2).iterator();
			}			
		});
		
		rdd2.foreach(new VoidFunction<String>(){
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});

		// foreachPartitions
		rdd1.foreachPartition(new VoidFunction<Iterator<String>>() {
			public void call(Iterator<String> t) throws Exception {
				System.out.println(t.next()+"~~~~~~");
			}
		});
		rdd1.foreach(new VoidFunction<String>(){
			public void call(String t) throws Exception {
				System.out.println(t+"^^^^^^^");
			}
		});
		
		//filter 后 coalesce
		JavaPairRDD<String,Integer> rdd4 = basicRDD.filter(new Function<Tuple2<String,Integer>,Boolean>(){
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				if(v1._1.startsWith("A")){
					return true;		
				}	
				return false;
			}
		});
		rdd4.coalesce(1);
		
		// repartitionAndSortWithinPartitions 替代 repartition 与sort类操作
		// 一边repartition 一边 sort
		basicRDD.foreach(new VoidFunction<Tuple2<String,Integer>>(){
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+"----11111---"+t._2);
			}
		});
		System.out.println("---num partition---"+basicRDD.getNumPartitions());
		JavaPairRDD<String,Integer> rdd6 = basicRDD.repartitionAndSortWithinPartitions(new Partitioner() {
			public int getPartition(Object key) {
				return key.toString().split(" ").length % numPartitions();
			}

			public int numPartitions() {
				return 2;
			}	
		});
		rdd6.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>(){
			public void call(Iterator<Tuple2<String, Integer>> t) throws Exception {
				System.out.println(t.next()._1+"---000----"+t.next()._2);
			}
		});
	}
}
