package bigdata;

import java.io.Serializable;

public class Distribution implements Serializable {

    private final String name;
    private double min;
    private double max;
    private double avg;
    private long med;
    private long quar1;
    private long quar3;
    private long count;
    private long[] histogram;

    Distribution(String name){
        this.name = name;
        this.min = -1;
        this.max = -1;
        this.avg = -1;
        this.med = -1;
        this.quar1 = -1;
        this.quar3 = -1;
    }

    public double getMin() {
        return this.min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return this.max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getAvg() {
        return this.avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public long getMed() {
        return this.med;
    }

    public void setMed(long med) {
        this.med = med;
    }

    public long getQuar1() {
        return this.quar1;
    }

    public void setQuar1(long quar1) {
        this.quar1 = quar1;
    }

    public long getQuar3() {
        return this.quar3;
    }

    public void setQuar3(long quar3) {
        this.quar3 = quar3;
    }

    public long getCount() {
        return this.count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long[] getHistogram() {
        return this.histogram;
    }

    public void setHistogram(long[] histogram) {
        this.histogram = histogram;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
		sb.append("-----------------------------------------------\n");
		sb.append(this.name + "\n");
		sb.append("-----------------------------------------------\n");
		sb.append("count is = " + getCount());
		sb.append("\nmin is = " + (long) getMin());
		sb.append("\nmax is = " + (long) getMax());
		sb.append("\naverage is = " + (long) getAvg());
		sb.append("\nmedian is = " + getMed());
		sb.append("\n1st quartile is = " + getQuar1());
        sb.append("\n3rd quartile is = " + getQuar3());
        sb.append("\nhistogram is = [\n10 100 1000 10000 100000 1000000\n");
        for (int n = 0; n < this.histogram.length; ++n){
            sb.append(this.histogram[n] + " ");
        }
        sb.append("\n]\n\n");
        return sb.toString();
    }
}