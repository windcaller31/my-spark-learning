package com.lmt.basic;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Multiset.Entry;

import scala.Tuple2;

//避免重复创建RDD
public class Basic1 {
	public static void main(String args[]){
		SparkConf conf = new SparkConf().setAppName("basic1").setMaster("local[*]");
		// SparkConf conf = new SparkConf().setAppName("sparkzip");
		Configuration config = new Configuration();
		JavaSparkContext sc = new JavaSparkContext(conf);
		//rdd1 创建了一次
		JavaRDD<String> rdd1 = sc.textFile("/Users/iyx/Documents/workspace/SparkLearning/TestFile/test.txt");
		JavaRDD<String> finalRdd_0_0 = rdd1.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		JavaPairRDD<String,Integer> finalRdd_0_1 = finalRdd_0_0.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t,1);
			}
		});
		Map<String,Long> map_0 = finalRdd_0_1.countByKey();
		int i = 0;
		for(String s : map_0.keySet() ){
			System.out.println(s);
			if(i==10){
				break;
			}
			i++;
		}
		
		//rdd2 又创建了一次 这里直接用rdd1即可
		JavaRDD<String> rdd2 = sc.textFile("/Users/iyx/Documents/workspace/SparkLearning/TestFile/test.txt");
		JavaRDD<String> finalRdd_1_0 = rdd2.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		JavaPairRDD<String,Integer> finalRdd_1_1 = finalRdd_1_0.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t,1);
			}
		});
		Map<String,Long> map_1 = finalRdd_1_1.countByKey();
		int j = 0;
		for(String s : map_1.keySet() ){
			System.out.println(s);
			if(j==10){
				break;
			}
			j++;
		}
	}
}
