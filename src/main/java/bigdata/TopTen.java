package bigdata;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

public class TopTen {

    List<Tuple2<String, Long>> topten;
    String name;

    TopTen(List<Tuple2<String, Long>> top_ten, String name) {
        this.topten = new ArrayList<Tuple2<String, Long>>(top_ten);
        this.name = name;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
		sb.append("-----------------------------------------------\n");
		sb.append("TOP TEN " + this.name + "\n");
		sb.append("-----------------------------------------------\n");
        for (int n = 0; n < this.topten.size(); ++n){
            sb.append(Integer.toString(n + 1) + ". " + this.topten.get(n)._1 + "   " + Long.toString(this.topten.get(n)._2) + "\n");
        }
        return sb.toString();
    }
}