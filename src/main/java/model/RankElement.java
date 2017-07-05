package model;

/**
 * Created by marco on 05/07/17.
 */
public class RankElement implements Comparable<RankElement>{

    private String player;
    private Double speed;

    public RankElement(String player,Double speed){
        this.player = player;
        this.speed = speed;
    }


    public String getPlayer() {
        return player;
    }

    public void setPlayer(String player) {
        this.player = player;
    }

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    @Override
    public int compareTo(RankElement o) {
        if (this.speed > o.getSpeed())
            return -1;
        else if (this.speed < o.getSpeed())
            return 1;
        else
            return (this.getPlayer().compareTo(o.getPlayer()));
    }

    @Override
    public String toString() {
        return "RankElement{" +
                "player='" + player + '\'' +
                ", speed=" + speed +
                '}';
    }
}
