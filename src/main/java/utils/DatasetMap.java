package utils;

import java.util.HashMap;

/**
 * Created by marco on 24/06/17.
 */
public class DatasetMap {

    private static HashMap<Long, String> datasetMap = null;

    public static void initMap(){
        if (datasetMap == null){
            datasetMap = new HashMap<>();
            /**
             * TEAM A
             */
            //Nick Gertje (Left Leg: 13, Right Leg: 14, Left Arm: 97, Right Arm: 98)
            datasetMap.put(13L,"Nick Gertje");
            datasetMap.put(14L,"Nick Gertje");
            datasetMap.put(97L,"Nick Gertje");
            datasetMap.put(98L,"Nick Gertje");
            //Dennis Dotterweich (Left Leg: 47, Right Leg:16)
            datasetMap.put(47L,"Dennis Dotterweich");
            datasetMap.put(16L,"Dennis Dotterweich");
           //Niklas Waelzlein (Left Leg: 49, Right Leg: 88)
            datasetMap.put(49L,"Niklas Waelzlein");
            datasetMap.put(88L,"Niklas Waelzlein");
           //Wili Sommer (Left Leg: 19, Right Leg: 52)
            datasetMap.put(19L,"Wili Sommer");
            datasetMap.put(52L,"Wili Sommer");
           //Philipp Harlass (Left Leg: 53, Right Leg: 54)
            datasetMap.put(53L,"Philipp Harlass");
            datasetMap.put(54L,"Philipp Harlass");
           //Roman Hartleb (Left Leg: 23, Right Leg: 24)
            datasetMap.put(23L,"Roman Hartleb");
            datasetMap.put(24L,"Roman Hartleb");
           //Erik Engelhardt (Left Leg: 57, Right Leg: 58)
            datasetMap.put(57L,"Erik Engelhardt");
            datasetMap.put(58L,"Erik Engelhardt");
           //Sandro Schneider (Left Leg: 59, Right Leg: 28)
            datasetMap.put(59L,"Sandro Schneider");
            datasetMap.put(28L,"Sandro Schneider");

            /***
             * TEAM B
             */
            //Leon Krapf (Left Leg: 61, Right Leg: 62, Left Arm: 99, Right Arm: 100)
            datasetMap.put(61L,"Leon Krapf");
            datasetMap.put(62L,"Leon Krapf");
            datasetMap.put(99L,"Leon Krapf");
            datasetMap.put(100L,"Leon Krapf");
            //Kevin Baer (Left Leg: 63, Right Leg: 64)
            datasetMap.put(63L,"Hand");
            datasetMap.put(64L,"Hand");
            //Luca Ziegler (Left Leg: 65, Right Leg: 66)
            datasetMap.put(65L,"Luca Ziegler");
            datasetMap.put(66L,"Luca Ziegler");
            //Ben Mueller (Left Leg: 67, Right Leg: 68)
            datasetMap.put(67L,"Ben Mueller");
            datasetMap.put(68L,"Ben Mueller");
            //Vale Reitstetter (Left Leg: 69, Right Leg: 38)
            datasetMap.put(69L,"Vale Reitstetter");
            datasetMap.put(38L,"Vale Reitstetter");
            //Christopher Lee (Left Leg: 71, Right Leg: 40)
            datasetMap.put(71L,"Christopher Lee");
            datasetMap.put(40L,"Christopher Lee");
            //Leon Heinze (Left Leg: 73, Right Leg: 74)
            datasetMap.put(73L,"Leon Heinze");
            datasetMap.put(74L,"Leon Heinze");
            //Leo Langhans (Left Leg: 75, Right Leg: 44)
            datasetMap.put(75L,"Leo Langhans");
            datasetMap.put(44L,"Leo Langhans");

            /**
             * REFEREE
             */
            //Referee (Left Leg: 105, Right Leg: 106)
            datasetMap.put(105L,"Referee");
            datasetMap.put(106L,"Referee");

            /**
             * BALLS
             */
            //- 1st Half: 4, 8, 10
            //- 2nd Half: 4, 8, 10, 12
            datasetMap.put(4L,"Ball");
            datasetMap.put(4L,"Ball");
            datasetMap.put(10L,"Ball");
            datasetMap.put(12L,"Ball");

        }
    }

    public static HashMap<Long, String> getDatasetMap() {
        return datasetMap;
    }
}
