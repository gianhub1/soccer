package model;

/**
 * Created by marco on 07/07/17.
 */
public class HeatMap {

    private Integer cell_id;
    private Double percent_time;

    public HeatMap(){}

    public HeatMap(Integer cell_id,Double percent_time){
        this.cell_id = cell_id;
        this.percent_time = percent_time;
    }

    public Integer getCell_id() {
        return cell_id;
    }

    public void setCell_id(Integer cell_id) {
        this.cell_id = cell_id;
    }

    public Double getPercent_time() {
        return percent_time;
    }

    public void setPercent_time(Double percent_time) {
        this.percent_time = percent_time;
    }

    @Override
    public String toString() {
        return "HeatMap{" +
                "cell_id=" + cell_id +
                ", percent_time=" + percent_time +
                '}';
    }
}
