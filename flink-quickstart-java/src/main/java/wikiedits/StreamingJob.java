/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wikiedits;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		TextInputFormat format = new TextInputFormat(new org.apache.flink.core.fs.Path("D:/flinkText"));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        DataStream<String> text = env.readFile(format, "D:/flinkText", FileProcessingMode.PROCESS_ONCE, 100);
        //DataStream<String> text = env.readTextFile("D:/flinkText/flink.txt");
        DataStream<Tuple2<String, Integer>> withCountDataStream = text
                .flatMap(
                        new Splitter()
                )
                .keyBy(0)
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> {
                    return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                });

        withCountDataStream.print();
        withCountDataStream.writeAsText("D:/result", FileSystem.WriteMode.OVERWRITE);


//        DataStream<Long> someIntegers = env.generateSequence(0, 10);
//
//        DataStream<Integer> myInts = env.fromElements(1,2,3,4,56);
//
//        List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("yzw",1), new Tuple2<>("yzw1",2));
//        DataStream<Tuple2<String, Integer>> myTuples = env.fromCollection(data);
//
//        IterativeStream<Long> iteration = someIntegers.iterate();
//
//        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
//            @Override
//            public Long map(Long value) throws Exception {
//                return value - 1 ;
//            }
//        });
//
//
//        //过滤给下游数据的节点数据
//        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
//            @Override
//            public boolean filter(Long value) throws Exception {
//                return (value > 0);
//            }
//        });
//
//        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
//            @Override
//            public boolean filter(Long value) throws Exception {
//                return (value <= 0);
//            }
//        });
//        //如果给两个 则是并行的输出结果
//        iteration.closeWith(stillGreaterThanZero);
//        iteration.closeWith(lessThanZero);  //lessThanZero 输出给下游节点 -1,0,-2,-1输出给 stillGreaterThanZero 由于filter把他过滤点 所以不会有输出
//        //minusOne.print();
//        //stillGreaterThanZero.print();
//        lessThanZero.print();
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
	}



    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
