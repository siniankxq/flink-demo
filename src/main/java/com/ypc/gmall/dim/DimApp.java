package com.ypc.gmall.dim;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ypc.bean.TableProcess;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;

/**
 * @author xun (siniankxq@163.com)
 * @version 1.0.0
 * @ClassName DimApp.java
 * @Description
 * @createTime 2024年01月30日 10:49:00
 */
public class DimApp {

	private static final String bootstrapServers = "175.27.155.249:51137";
	private static final String TOPIC = "topic_db";

	private static final String GROUP_ID = "topic_db-01";


	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		streamExecutionEnvironment.setStateBackend(new HashMapStateBackend());
		//设置检查点
		streamExecutionEnvironment.enableCheckpointing(5000);
		streamExecutionEnvironment.setParallelism(1);

		// TODO 2. 开启检查点   Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,
		// 需要从Checkpoint或者Savepoint启动程序
		// 2.1 开启Checkpoint,每隔5秒钟做一次CK  ,并指定CK的一致性语义
		streamExecutionEnvironment.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
		// 2.2 设置超时时间为 1 分钟
		streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
		// 2.3 设置两次重启的最小时间间隔
		streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
		// 2.4 设置任务关闭的时候保留最后一次 CK 数据
		streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(
				CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		// 2.5 指定从 CK 自动重启策略
		streamExecutionEnvironment.setRestartStrategy(RestartStrategies.failureRateRestart(
				3, Time.days(1L), Time.minutes(1L)));

		streamExecutionEnvironment.getCheckpointConfig().setCheckpointStorage(
				"hdfs://hadoop102:8020/flinkCDC"
		);
//		Properties properties = new Properties();
//
//		properties.setProperty("bootstrap.servers", bootstrapServers);
//		properties.setProperty("group.id", GROUP_ID);
//		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		properties.setProperty("auto.offset.reset", "earliest");
//
////		new SimpleStringSchema()
//		FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(TOPIC, new KafkaDeserializationSchema<String>() {
//			@Override
//			public boolean isEndOfStream(String nextElement) {
//				return false;
//			}
//
//			@Override
//			public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
//				if (record.value() == null || record.value() == null || record.value().length == 0) {
//					return null;
//				}
//				return new String(record.value());
//			}
//
//			@Override
//			public TypeInformation<String> getProducedType() {
////				return BasicTypeInfo.STRING_TYPE_INFO;
//				return TypeInformation.of(String.class);
//			}
//		}, properties);
//
//		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//				.setBootstrapServers(bootstrapServers)
//				.setTopics(TOPIC)
//				.setGroupId(GROUP_ID)
//				.setStartingOffsets(OffsetsInitializer.latest())
//				.setValueOnlyDeserializer(new SimpleStringSchema())
//				.build();
//
//
//		OutputTag<String> notJsonTag = new OutputTag<String>("notJsonTag"){
//
//		};
////		DataStreamSource<String> topicDbStreamSource = streamExecutionEnvironment.addSource(flinkKafkaConsumer);
//
		WatermarkStrategy<String> objectWatermarkStrategy = WatermarkStrategy.noWatermarks();
//
//		DataStreamSource<String> topicDbStreamSource = streamExecutionEnvironment.fromSource(kafkaSource, objectWatermarkStrategy, "topic_db-01");
//		//过滤掉非json数据，写入侧输出流
//		SingleOutputStreamOperator<JSONObject> singleOutputStreamOperator = topicDbStreamSource.process(new ProcessFunction<String, JSONObject>() {
//			@Override
//			public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//				try {
//
//					JSONObject jsonObject = JSON.parseObject(value);
//					out.collect(jsonObject);
//					System.out.println("解析成功");
//					return;
//				} catch (Exception e) {
//					ctx.output(notJsonTag, value);
//					return;
//				}
//			}
//		});
//
//		DataStream<String> notJsonStream = singleOutputStreamOperator.getSideOutput(notJsonTag);
//		notJsonStream.print();


		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
				.hostname("175.27.155.249")
				.port(51126)
				.username("root")
				.password("QQ2011com!")
				.databaseList("gmall-config")
				.tableList("gmall-config.table_process")
				.deserializer(new JsonDebeziumDeserializationSchema())
				.startupOptions(StartupOptions.initial())
				.build();

		DataStream<String> configStream = streamExecutionEnvironment.fromSource(mySqlSource, objectWatermarkStrategy, "config-source");
		// 将配置流进行广播
//		BroadcastStream<String> broadcastConfigStream = configStream.broadcast(new MapStateDescriptor<>("config", String.class, String.class));
//		BroadcastConnectedStream<JSONObject, String> connectedStream = singleOutputStreamOperator.connect(broadcastConfigStream);
//		connectedStream.process(new BroadcastProcessFunction<JSONObject, String, JSONObject>() {
//			@Override
//			public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//				System.out.println("配置表信息value = " + value);
//			}
//
//			@Override
//			public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
//				System.out.println("处理数据");
//			}
//
//
//		});

		configStream.print();
		streamExecutionEnvironment.execute("dimApp");
	}
}
