package wikiedits;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(
                        (WikipediaEditEvent editEvent) -> editEvent.getUser()
                );

        DataStream<Tuple2<String, Integer>> result = keyedEdits.timeWindow(Time.seconds(5)).aggregate(
                new AggregateFunction<WikipediaEditEvent, Tuple2<String, Integer>, Tuple2<String,Integer>>(){

            @Override
            public Tuple2<String, Integer> createAccumulator() {
                return new Tuple2<>("",0);
            }

            @Override
            public Tuple2<String, Integer> add(WikipediaEditEvent wikipediaEditEvent, Tuple2<String, Integer> stringIntegerTuple2) {
                return new Tuple2<>(wikipediaEditEvent.getUser(), wikipediaEditEvent.getByteDiff()+ stringIntegerTuple2.f1);
            }

            @Override
            public Tuple2<String, Integer> getResult(Tuple2<String, Integer> stringIntegerTuple2) {
                return stringIntegerTuple2;
            }

            @Override
            public Tuple2<String, Integer> merge(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> acc1) {
                return new Tuple2<>(stringIntegerTuple2.f0 + stringIntegerTuple2.f0, stringIntegerTuple2.f1 + stringIntegerTuple2.f1);
            }
        });

        result.map((Tuple2<String,Integer> map) -> map.toString()).addSink(new FlinkKafkaProducer010<>("localhost:9092", "wiki-result", new SimpleStringSchema()));

        result.print();

        see.execute("Wikipedia User Edit Volume");

    }

}
