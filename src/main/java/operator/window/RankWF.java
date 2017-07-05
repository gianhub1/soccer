package operator.window;

import model.RankElement;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.TreeSet;

/**
 * Created by marco on 05/07/17.
 */
public class RankWF implements AllWindowFunction<Tuple3<Long,Long,TreeSet<RankElement>>,Tuple3<Long,Long,TreeSet<RankElement>>,Window> {


    @Override
    public void apply(Window window, Iterable<Tuple3<Long, Long, TreeSet<RankElement>>> iterable, Collector<Tuple3<Long, Long, TreeSet<RankElement>>> collector) throws Exception {
        Tuple3<Long, Long,TreeSet<RankElement>> lastestTuple = iterable.iterator().next();
        collector.collect(lastestTuple);
    }
}