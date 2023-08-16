package com.ypc.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author xun (siniankxq@163.com)
 * @version 1.0.0
 * @ClassName WordCountStreamDemo.java
 * @Description
 * @createTime 2023年08月16日 16:11:00
 */
public class WordCountStreamDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		streamExecutionEnvironment.setParallelism(4);
		DataStreamSource<String> lineDataStreamSource = streamExecutionEnvironment.readTextFile("word.txt");

		SingleOutputStreamOperator<Tuple2<String, Integer>> wordSingleOutputStreamOperator = lineDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
				String[] words = line.split(" ");
				for (String word : words) {
					collector.collect(Tuple2.of(word, 1));
				}

			}
		});

//		SingleOutputStreamOperator<Tuple2<String, Integer>> wordSingleOutputStreamOperator = lineDataStreamSource.flatMap((String line, Collector<String> words) -> {
//					Arrays.stream(line.split(" ")).forEach(words::collect);
//				})
//				.returns(Types.STRING)
//				.map(word -> Tuple2.of(word, 1))
//				.returns(Types.TUPLE(Types.STRING, Types.INT));

		KeySelector<Tuple2<String, Integer>, String> keySelector = new KeySelector<Tuple2<String, Integer>, String>() {

			@Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				return value.f0;
			}
		};

		wordSingleOutputStreamOperator
				.keyBy(keySelector)
				.sum(1).print();

		streamExecutionEnvironment.execute();
	}
}
