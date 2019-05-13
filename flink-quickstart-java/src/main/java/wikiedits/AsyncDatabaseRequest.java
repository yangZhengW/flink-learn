package wikiedits;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class AsyncDatabaseRequest extends RichAsyncFunction<String, Tuple2<String, String>>{
    @Override
    public void asyncInvoke(String s, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {

    }

    @Override
    public void timeout(String input, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {

    }
}
