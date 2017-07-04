package core;

import configuration.AppConfiguration;
import kafka.KafkaConnectors;
import model.SensorData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import utils.DatasetMap;

import java.io.FileReader;
import java.io.Reader;

/**
 * Created by marco on 24/06/17.
 */
public class WriteDatasetToKafka {



    public static void main(String[] args) throws Exception {

        SensorData sensorData;
        DatasetMap.initMap();
        Reader in = new FileReader(AppConfiguration.OUTPUT_FILE);
        //Writer out = new FileWriter(AppConfiguration.OUTPUT_FILE);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
        int i = 0 ;
        long ts = 0;
        for (CSVRecord record : records) {
/*
            if (!isNotPlayer(Long.parseLong(record.get(0))) && !prePostMatchEvent(Long.parseLong(record.get(1))/10000)){
*/
                sensorData = new SensorData(Long.parseLong(record.get(0)),Long.parseLong(record.get(1))/100000,Long.parseLong(record.get(2)),
                        Long.parseLong(record.get(3)),Long.parseLong(record.get(4)));
                if (i == 0)
                    ts = sensorData.getTs();
                if (i> 0 )
                    sensorData.setTs(sensorData.getTs() - ts);
                /*out.write(record.get(0)+","
                        +String.valueOf(((Long.parseLong(record.get(1))/10000)))+
                        ","+record.get(2)+","+record.get(3)+","
                        +record.get(5)+"\n");
                out.flush();*/
                KafkaConnectors.kafkaProducer(AppConfiguration.TOPIC, AppConfiguration.KEY,AppConfiguration.PRODUCER_KAFKA_BROKER,sensorData);
                i++;
           // }
        }
        System.out.println(i);
        //KafkaConnectors.kafkaProducer(AppConfiguration.TOPIC, AppConfiguration.KEY,AppConfiguration.PRODUCER_KAFKA_BROKER,sensorData);
        System.exit(0);

    }

    public static boolean isNotPlayer(long sid){
        return ((DatasetMap.getDatasetMap().get(sid)==null || DatasetMap.getDatasetMap().get(sid).equals("Ball")
                || DatasetMap.getDatasetMap().get(sid).equals("Hand") || DatasetMap.getDatasetMap().get(sid).equals("Referee")));
    }

    public static boolean prePostMatchEvent(long timestamp){
        return (timestamp < AppConfiguration.TS_MATCH_START || timestamp > AppConfiguration.TS_MATCH_STOP);
    }

}
