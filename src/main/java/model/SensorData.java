package model;


public class SensorData {



    private long ts;
    private long x;
    private long y;
    private double v;
    private String key;
    private long sid;

    public SensorData(){

    }

    public SensorData(String key,long ts, long x, long y, double v,long sid){
        this.key = key;
        this.ts=ts;
        this.x=x;
        this.y=y;
        this.v=v;
        this.sid = sid;

    }


    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public long getX() {
        return x;
    }

    public void setX(long x) {
        this.x = x;
    }

    public long getY() {
        return y;
    }

    public void setY(long y) {
        this.y = y;
    }

    public double getV() {
        return v;
    }

    public void setV(double v) {
        this.v = v;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "ts=" + ts +
                ", x=" + x +
                ", y=" + y +
                ", v=" + v +
                ", key='" + key + '\'' +
                '}';
    }

    public Double computeDistance(double x, double y) {
        return Math.sqrt(x*x + y*y);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getSid() {
        return sid;
    }

    public void setSid(long sid) {
        this.sid = sid;
    }
}
