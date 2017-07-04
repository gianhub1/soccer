package configuration;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by marco on 24/06/17.
 */
public class AppConfiguration {

    public static final String FILENAME = "/Users/marco/Desktop/application.properties";
    public static final long TS_MATCH_START = 10753295594424116L;
    public static final long TS_MATCH_STOP = 14879639146403495L;
    public static final String OUTPUT_FILE = "/Users/marco/Desktop/dataset";

    // watermark
    public static long WATERMARK_INTERVAL = 1;

    // dataset
    public static String DATASET_FILE = "/Users/marco/Downloads/full-game";

    // zookeeper host & kafka broker & topic
    public static String CONSUMER_ZOOKEEPER_HOST = "localhost:2181";
    public static String CONSUMER_KAFKA_BROKER = "localhost:9092";
    public static String PRODUCER_KAFKA_BROKER = "localhost:9092";
    public static String TOPIC = "test";
    public static String KEY = "test";


    public static void readConfiguration() {

        try {
            Properties prop = new Properties();

            FileInputStream inputStream = new FileInputStream(FILENAME);

            prop.load(inputStream);

            // watermark
            WATERMARK_INTERVAL = Long.parseLong(prop.getProperty("WATERMARK_INTERVAL"));

            // set tuple for test
            DATASET_FILE = prop.getProperty("DATASET_FILE");

            // zookeeper host & kafka broker
            CONSUMER_ZOOKEEPER_HOST = prop.getProperty("CONSUMER_ZOOKEEPER_HOST");
            CONSUMER_KAFKA_BROKER = prop.getProperty("CONSUMER_KAFKA_BROKER");
            PRODUCER_KAFKA_BROKER = prop.getProperty("PRODUCER_KAFKA_BROKER");
            TOPIC = prop.getProperty("TOPIC");
            KEY = prop.getProperty("KEY");



        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
