package core;

import configuration.AppConfiguration;
import model.SensorData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import utils.DatasetMap;
import utils.KafkaConnectors;

import java.io.FileReader;
import java.io.Reader;


public class WriteToKafka {



    public static void main(String[] args) throws Exception {

        SensorData sensorData;
        DatasetMap.initMap();
        Reader in = new FileReader(AppConfiguration.FILTERED_DATASET_FILE);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
        for (CSVRecord record : records) {

            sensorData = new SensorData(record.get(1),Long.parseLong(record.get(2)),
                    Long.parseLong(record.get(3)),Long.parseLong(record.get(4)),Double.parseDouble(record.get(5)),Long.parseLong(record.get(0)));
            KafkaConnectors.kafkaProducer(AppConfiguration.TOPIC, AppConfiguration.KEY,AppConfiguration.PRODUCER_KAFKA_BROKER,sensorData);
        }
        Thread.sleep(10000);
        sensorData = new SensorData("END",0L,0L,0L,0d,0L);
        KafkaConnectors.kafkaProducer(AppConfiguration.TOPIC, AppConfiguration.KEY,AppConfiguration.PRODUCER_KAFKA_BROKER,sensorData);
        System.exit(0);

    }

}