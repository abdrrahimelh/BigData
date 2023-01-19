package Radar;

import Helper.Helper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RadarTagmaster1 {
    private static boolean isValidDate(String horodate,  String centieme){
        String[] date = horodate.split(" ")[0].split("/");
        int jour = Integer.parseInt(date[0]);
        int mois = Integer.parseInt(date[1]);
        String[] time = horodate.split(" ")[1].split(":");
        int heure = Integer.parseInt(time[0]);
        int minutes = Integer.parseInt(time[1]);
        int secondes = Integer.parseInt(time[2]);
        return (jour >= 1 && jour <= 31) && (mois >= 1 && mois <= 12) &&
                (heure >= 1 && heure <= 23) && (minutes >= 1 && minutes <= 59) &&
                (secondes >= 1 && secondes <= 59);
    }

    private static boolean isValidType(String type){
        return type.startsWith("VL") || type.startsWith("2RM") ||
                type.startsWith("PL") || type.equals("Bus") || type.equals("Deux roues");
    }

    private static boolean isValidDirection(String direction){
        List<String> validDirections = Arrays.asList("1", "2", "Sortie fac", "Entrée fac");
        return validDirections.contains(direction);
    }

    public static boolean isValid(String str) {
        String[] tokens = str.split(",");
        if (tokens.length != 6) // or 7
            return false;
        try {
            if(!isValidDirection(tokens[2])) return false;
            if(!isValidDate(tokens[0], tokens[1])) return false;
            if(!isValidType(tokens[5])) return false;
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private static String adaptDirection(String direction) {
        if (direction.equals("Sortie fac")) return "2";
        if (direction.equals("Entrée fac")) return "1";
        return direction;
    }

    private static String adaptDate(String horodate, String centime) {
        String[] date = horodate.split(" ")[0].split("/");
        String heure = horodate.split(" ")[1];
        return date[0] + "," + date[1] + "/" + date[2] + "," + heure + ":" + centime;
    }

    private static String adaptType(String type) {
        if (type.startsWith("VL")) return "VL";
        if (type.startsWith("2RM")) return "2RM";
        if (type.startsWith("PL")) return "PL";
        if (type.equals("Bus")) return "PL";
        if (type.equals("Deux roues")) return "2RM";
        return type;
    }

    // CAPTEUR(P?),TYPECAPTEUR,SENS,JOUR,MOIS/ANNEE,HEURE:MINUTE:SECONDE:CENTIEME,VITESSE,TYPE VEHICULE
    private static String adapt(String line, String fileName){
        String[] tokens = line.split(",");
        String str = "";
        str += Helper.getPosition(fileName);
        str+=",";
        str += "RADAR_TAGMASTER";
        str += adaptDirection(tokens[2]) + ",";
        str += adaptDate(tokens[0], tokens[1]) + ",";
        str += tokens[4] + ","; //vitesse
        str += adaptType(tokens[5]) + ",";
        str += tokens[3] ;
        return str;
    }

    public static class RadarMapper
            extends Mapper<Object, Text, NullWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String fileName = conf.get("fileName");
            String line = value.toString();
            String[] tokens = line.split(",");
            if(isValid(line)){
                context.write(NullWritable.get(), new Text(adapt(line, fileName)));
            }
        }
    }
}
