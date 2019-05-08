package wikiedits;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import scala.Int;

public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>{
    private transient ValueState<Tuple2<Integer,Integer>> sum;
    public static void main(String[] args){

    }


    @Override
    public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
            Tuple2<Integer,Integer> currentsum = sum.value();
            currentsum.f0 += 1;
            currentsum.f1 += input.f1;
            sum.update(currentsum);
            if(currentsum.f0 > 2){
                collector.collect(new Tuple2<>(input.f0, currentsum.f1 / currentsum.f0));
                sum.clear();
            }
    }

    @Override
    public void open(Configuration config) {
//        ValueStateDescriptor<Tuple2<Integer,Integer>> descriptor =
//                new ValueStateDescriptor<>(
//                        "averager", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), Tuple2.of(0L, 0L));

        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {}), // type information
                        Tuple2.of(0, 0)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }

}
