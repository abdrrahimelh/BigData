package Radar;

import Helper.Helper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RadarTagmaster2 implements Cleaner {
    @Override
    public boolean isValid(String str) {
        String[] tokens = str.split(",");
        if (tokens.length != 8)
            return false;
        try {
            if(!isValidDirection(tokens[2])) return false;
            if(!isValidDate(tokens[0], tokens[1])) return false;
            if(!isValidType(tokens[7])) return false;
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public String adapt(String line, String fileName){
        String[] tokens = line.split(",");
        String str = "";
        str += Helper.getPosition(fileName) + ","; //CAPTEUR(P?)
        str += "RADAR_TAGMASTER" + ","; //TYPECAPTEUR
        str += adaptDirection(tokens[2]) + ","; //SENS
        str += adaptDate(tokens[0], tokens[1]) + ","; //JOUR,MOIS/ANNEE,HEURE:MINUTE:SECONDE:CENTIEME
        str += tokens[3] + ","; //VITESSE
        str += adaptType(tokens[7]); //TYPE VEHICULE
        return str;
    }

    private boolean isValidDate(String horodate,  String centieme){
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

    private boolean isValidType(String type){
        return type.startsWith("VL") || type.startsWith("2RM") ||
                type.startsWith("PL") || type.equals("Bus") || type.equals("Deux roues");
    }

    private boolean isValidDirection(String direction){
        return direction.equals("1") || direction.equals("2") ||
                direction.contains("Entr") || direction.contains("Sortie") ||
                direction.contains("entr") || direction.contains("sortie");
    }

    private String adaptDirection(String direction) {
        if (direction.contains("Sortie") || direction.contains("sortie")) return "2";
        if (direction.contains("Entr") || direction.contains("entr")) return "1";
        return direction;
    }

    private String adaptDate(String horodate, String centime) {
        String[] date = horodate.split(" ")[0].split("/");
        String heure = horodate.split(" ")[1];
        return date[0] + "," + date[1] + "/" + date[2] + "," + heure + ":" + centime;
    }

    private String adaptType(String type) {
        if (type.startsWith("VL")) return "VL";
        if (type.startsWith("2RM") || type.equals("Deux roues")) return "2R";
        if (type.startsWith("PL") || type.equals("Bus")) return "PL";
        return type;
    }
}