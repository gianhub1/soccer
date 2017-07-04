package model;

/**
 * Created by marco on 24/06/17.
 */
public class SensorData {



    private long sid;
    private long ts;
    private long x;
    private long y;
    private double v;

    public SensorData(){

    }

    public SensorData(long sid, long ts, long x, long y, long v){
        this.sid=sid;
        this.ts=ts;
        this.x=x;
        this.y=y;
        this.v=v;

    }

    public long getSid() {
        return sid;
    }

    public void setSid(long sid) {
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
                "sid=" + sid +
                ", ts=" + ts +
                ", x=" + x +
                ", y=" + y +
                ", v=" + v +
                '}';
    }

    public Double computeDistance(double x, double y) {
        return Math.sqrt(x*x + y*y);
    }
}
