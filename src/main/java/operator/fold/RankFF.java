package operator.fold;

import model.RankElement;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.TreeSet;


public class RankFF implements FoldFunction<Tuple5<Long,Long,String,Double,Double>, Tuple3<Long, Long,TreeSet<RankElement>>> {



    @Override
    public Tuple3<Long, Long, TreeSet<RankElement>> fold(Tuple3<Long, Long,TreeSet<RankElement>> in,
                                                            Tuple5<Long,Long,String,Double,Double> tuple) throws Exception {
        if(in.f2 != null) {
            in.f2.add(new RankElement(tuple.f2,tuple.f4));
            if (in.f2.size() > 5)
                in.f2.remove(in.f2.last());
            long stop_timestamp = 0;
            long start_timestamp = 0;
            if (in.f0 >= tuple.f0)
                start_timestamp = tuple.f0;
            else
                start_timestamp = in.f0;
            if (in.f1 >= tuple.f1)
                stop_timestamp = in.f1;
            else
                stop_timestamp = tuple.f1;
            return new Tuple3<>(start_timestamp,stop_timestamp,in.f2);
        }
        else{
            TreeSet<RankElement> treeSet = new TreeSet<>();
            treeSet.add(new RankElement(tuple.f2,tuple.f4));
            return new Tuple3<>(tuple.f0, tuple.f1,treeSet);
        }

    }

}
