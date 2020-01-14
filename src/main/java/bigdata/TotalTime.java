package bigdata;

import java.io.Serializable;

public class TotalTime implements Serializable {

    private final String name;
    private double time;

    TotalTime(String name){
        this.name = name;
        this.time = 0;
    }

    public void addTime(double duration){
        this.time += duration;
    }

    public double getTime(){
        return this.time;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("-----------------------------------------------\n");
        sb.append(this.name + "\n");
		sb.append("-----------------------------------------------\n");
        sb.append("total duration is = " + (this.time / (1000000.0 * 60.0 * 60.0)) + " hours\n");
        return sb.toString();
    }
}