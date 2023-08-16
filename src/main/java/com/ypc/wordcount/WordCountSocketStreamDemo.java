package com.ypc.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author xun (siniankxq@163.com)
 * @version 1.0.0
 * @ClassName WordCountSocketStreamDemo.java
 * @Description
 * @createTime 2023年08月16日 22:23:00
 */
public class WordCountSocketStreamDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> masterDS = env.socketTextStream("master", 11111);
		masterDS.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public void flatMap(String value, Collector<String> collector) throws Exception {
						Arrays.stream(value.split(" ")).forEach(collector::collect);
					}
				})
				.returns(Types.STRING)
				.map(word -> Tuple2.of(word, 1))
				.returns(Types.TUPLE(Types.STRING, Types.INT))
				.keyBy(value -> value.f0)
				.sum(1)
				.print();
		env.execute();

	}
}
