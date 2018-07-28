package com.lmt.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

//kyro 序列化
public class Basic6 {
	public static void main(String args[]) throws ClassNotFoundException {
		SparkConf conf = new SparkConf().setAppName("basic1").setMaster("local[*]");
		Configuration config = new Configuration();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(new Class<?>[] { Class.forName("com.lmt.basic.Basic6Po")});
		JavaSparkContext sc = new JavaSparkContext(conf);

	}
}
