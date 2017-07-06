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
public class FormatDataset {



    public static void main(String[] args) throws Exception {

        DatasetMap.initMap();
        Reader in = new FileReader(AppConfiguration.DATASET_FILE);
        Writer out = new FileWriter(AppConfiguration.OUTPUT_FILE+"_new");
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
        int i = 0 ;
        for (CSVRecord record : records) {

            if (!isInvalid(Long.parseLong(record.get(0))) && !prePostMatchEvent(Long.parseLong(record.get(1)))){

                out.write(record.get(0)+","+DatasetMap.getDatasetMap().get(Long.parseLong(record.get(0)))+","+Long.parseLong(record.get(1))/1000000000
                        +","+ Long.parseLong(record.get(2))/1000 + "," + (Long.parseLong(record.get(3)))/1000 + ","
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

}
