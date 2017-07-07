package core;

import configuration.AppConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import utils.DatasetMap;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;

/**
 * Created by marco on 24/06/17.
 */

//TODO DIMENSIONI IN MM DIVISIONE PER 1000
public class FormatDataset {



    public static void main(String[] args) throws Exception {

        DatasetMap.initMap();
        Reader in = new FileReader(AppConfiguration.FULL_DATASET_FILE);
        Writer out = new FileWriter(AppConfiguration.FILTERED_DATASET_FILE);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
        int i = 0 ;
        for (CSVRecord record : records) {
            if (!isInvalid(Long.parseLong(record.get(0))) && !prePostMatchEvent(Long.parseLong(record.get(1)))
                    && !inInterval(Long.parseLong(record.get(1)))){
                long x = Long.parseLong(record.get(2));
                long y = Long.parseLong(record.get(3));
                if (x < 0 )
                    x += 50;
                if (x > 52477)
                    x -= 11;
                if (y > 33941)
                    y -= 24;
                if (y < -33939)
                    y += 21;
                out.write(record.get(0)+","+DatasetMap.getDatasetMap().get(Long.parseLong(record.get(0)))+","+Long.parseLong(record.get(1))/1000000000
                        +","+ x + "," + y + ","
                        +  (Double.parseDouble(record.get(5))/1000000) + "\n" );
                out.flush();
                i++;
            }
        }
        System.out.println(i);
        System.exit(0);

    }

    public static boolean isInvalid(long sid){
        return ((DatasetMap.getDatasetMap().get(sid)==null || DatasetMap.getDatasetMap().get(sid).equals("Ball")
                || DatasetMap.getDatasetMap().get(sid).equals("Hand") || DatasetMap.getDatasetMap().get(sid).equals("Referee")));
    }

    public static boolean prePostMatchEvent(long timestamp){
        return (timestamp < AppConfiguration.TS_MATCH_START || timestamp > AppConfiguration.TS_MATCH_STOP);
    }

    public static boolean inInterval (long timestamp){
        return (timestamp > AppConfiguration.TS_INTERVAL_START && timestamp < AppConfiguration.TS_INTERVAL_STOP);
    }

}
