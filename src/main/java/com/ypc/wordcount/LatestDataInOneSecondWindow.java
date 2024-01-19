package com.ypc.wordcount;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class LatestDataInOneSecondWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DataEvent> dataStream = env.fromElements(
            new DataEvent("A", System.currentTimeMillis()),
            new DataEvent("B", System.currentTimeMillis() + 500),
            new DataEvent("C", System.currentTimeMillis() + 1000),
            new DataEvent("D", System.currentTimeMillis() + 1500)
        );

        // Assign timestamps and watermarks
        DataStream<DataEvent> dataStreamWithTimestamps = dataStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataEvent>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        DataStream<DataEvent> result = dataStreamWithTimestamps
            .keyBy(event -> event.getKey())
            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .process(new ProcessWindowFunction<DataEvent, DataEvent, String, TimeWindow>() {
                @Override
                public void process(String s, ProcessWindowFunction<DataEvent, DataEvent, String, TimeWindow>.Context context, Iterable<DataEvent> elements, Collector<DataEvent> out) throws Exception {

                }
            });

        result.print();

        env.execute("LatestDataInOneSecondWindow");
    }

    public static class DataEvent {
        private String key;
        private long timestamp;

        public DataEvent() {
        }

        public DataEvent(String key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        public String getKey() {
            return key;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
