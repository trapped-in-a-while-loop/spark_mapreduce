package bigdata;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

public class TopK {

    List<Tuple2<String, Long>> topk;
    String name;

    TopK(List<Tuple2<String, Long>> top_k, String name) {
        this.topk = new ArrayList<Tuple2<String, Long>>(top_k);
        this.name = name;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
		sb.append("-----------------------------------------------\n");
		sb.append("TOP K " + this.name + "\n");
		sb.append("-----------------------------------------------\n");
        for (int n = 0; n < this.topk.size(); ++n){
            sb.append(Integer.toString(n + 1) + ". " + this.topk.get(n)._1 + "   " + Long.toString(this.topk.get(n)._2) + "\n");
        }
        return sb.toString();
    }
}