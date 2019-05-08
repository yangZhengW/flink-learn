package wikiedits;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;

public class OperatorTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Tuple2<Integer,String>> dataStream = env.fromElements(Tuple2.of(1,"yzw"),Tuple2.of(1,"wolf"), Tuple2.of(3,"yzw1"), Tuple2.of(4,"yzw2"));
//        DataStream<String> result = dataStream.keyBy(0).fold("start", new FoldFunction<Tuple2<Integer,String>, String>() {
//                    @Override
//                    public String fold(String accumulator, Tuple2 value) throws Exception {
//                        System.out.println("key========" + value.f0);
//                        return accumulator + "-" +  value.f1;
//                    }
//                }
//        );

        //DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(Tuple2.of("yzw", 1), Tuple2.of("yzw", 2), Tuple2.of("yzl", 3));

        //processwindowExample
//        DataStream<String> text = env.socketTextStream("classb-bigdata0.server.163.org", 9010, "\n");
//        DataStream result = text.map(new MapFunction<String, Tuple2<String,Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String s) throws Exception {
//                String[] contents = s.split(",");
//                return Tuple2.of(contents[0],Integer.parseInt(contents[1]));
//            }
//        }).keyBy(t->t.f0).timeWindow(Time.seconds(10)).process(new MyProcessWindowFunction());

        //KeyedStream<Tuple2<String,Integer>, String> result =  dataStream.keyBy(t->t.f0);
        //KeyedStream<Tuple2<String, Integer>, Tuple> result1 =  dataStream.keyBy(0);

// processwindow combline with reducewindow
        DataStream<String> text = env.socketTextStream("classb-bigdata0.server.163.org", 9010, "\n");
        DataStream result = text.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] contents = s.split(",");
                return Tuple2.of(contents[0],Integer.parseInt(contents[1]));
            }
        }).keyBy(t->t.f0).timeWindow(Time.seconds(10)).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return t1.f1 > t2.f1 ? t2 : t1;
            }
        }, new MyProcessWindowFunction2());

        result.print();

        env.execute("myjob");

    }

    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> input, Collector<String> out) {
            Integer count = 0;
            for (Tuple2<String, Integer> in : input) {
                count++;
            }
            out.collect("Window: " + context.window() + "count: " + count);
        }
    }

    private static class MyProcessWindowFunction2
            extends ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> input, Collector<String> out) {
            Integer min = input.iterator().next().f1;
            out.collect(key + ":" + Tuple2.of(context.window().getStart(),min).toString());
        }
    }


    public static class AverageAggregate implements AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer,Integer>,Double>{
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0,0);
        }

        @Override
        public Tuple2<Integer, Integer> add(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<Integer, Integer> integerIntegerTuple2) {
            return new Tuple2<>(integerIntegerTuple2.f0 + stringIntegerTuple2.f1, integerIntegerTuple2.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {
            return ((double) integerIntegerTuple2.f0) / integerIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

}
