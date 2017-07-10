package configuration;

import java.io.FileInputStream;
import java.util.Properties;

import static java.lang.Math.abs;

/**
 * Created by marco on 24/06/17.
 */
public class AppConfiguration {

    public static final String FILENAME = "/Users/marco/Desktop/application.properties";
    public static final long TS_MATCH_START = 10753295594424116L;
    public static final long TS_MATCH_STOP = 14879639146403495L;
    public static final long TS_INTERVAL_START = 12557295594424116L;
    public static final long TS_INTERVAL_STOP = 13086639146403495L;
    public static final long OFFSET = 15 ;
    public static int VERTICAL_CEILS = 13;
    public static int HORIZONTAL_CEILS = 8;
    public static int X_MIN_FIELD = 0;
    public static int X_MAX_FIELD = 52477;
    public static int Y_MAX_FIELD = 33941;
    public static int Y_MIN_FIELD = -33939;
    public static int X_STEP = (abs(AppConfiguration.X_MIN_FIELD) + abs(AppConfiguration.X_MAX_FIELD))/AppConfiguration.HORIZONTAL_CEILS;
    public static int Y_STEP = (abs(AppConfiguration.Y_MIN_FIELD) + abs(AppConfiguration.Y_MAX_FIELD))/AppConfiguration.VERTICAL_CEILS;
    public static long MATCH_DURATION = (long) Math.ceil((((AppConfiguration.TS_MATCH_STOP-AppConfiguration.TS_MATCH_START)/1000000000)/1000)/60);

    public static String FILTERED_DATASET_FILE = "/Users/marco/Desktop/dataset";

    // watermark
    public static long WATERMARK_INTERVAL = 1000;

    // dataset
    public static String FULL_DATASET_FILE = "/Users/marco/Downloads/full-game";


    public static void readConfiguration() {

        try {
            Properties prop = new Properties();

            FileInputStream inputStream = new FileInputStream(FILENAME);

            prop.load(inputStream);

            // watermark
            WATERMARK_INTERVAL = Long.parseLong(prop.getProperty("WATERMARK_INTERVAL"));

            // set tuple for test
            FILTERED_DATASET_FILE = prop.getProperty("FILTERED_DATASET_FILE");
            FULL_DATASET_FILE = prop.getProperty("FULL_DATASET_FILE");




        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
