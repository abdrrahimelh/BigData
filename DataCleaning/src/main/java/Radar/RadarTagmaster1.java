package Radar;

import Helper.Helper;

import Prototype.DataCleaner;

public class RadarTagmaster1 implements DataCleaner {
    @Override
    public boolean isValid(String str) {
        String[] tokens = str.split(",");
        try {
            if(!isValidDate(tokens[0], tokens[1],tokens[2])) return false;
            if(!isValidType(tokens[5])) return false;
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public String adapt(String line, String fileName){
        String[] tokens = line.split(",");
        String str = "";
        str += Helper.getPosition(fileName);
        str+=",";
        str += "RADAR_TAGMASTER,";
        str += Helper.getDirectionsRadar(fileName) + ",";
        str += adaptDate(tokens[0], tokens[1],tokens[2],tokens[3]) + ",";
        str += tokens[4] + ","; //vitesse
        str += adaptType(tokens[5]);
        return str;
    }

    private boolean isValidDate(String date ,String heuree,String secondes){
        String[] datejour = date.split("/");
        int jour = Integer.parseInt(datejour[0]);
        int mois = Integer.parseInt(datejour[1]);
        String[] time = heuree.split(":");
        int heure = Integer.parseInt(time[0]);
        int minutes = Integer.parseInt(time[1]);
        int sec = Integer.parseInt(secondes);
        return (jour >= 1 && jour <= 31) && (mois >= 1 && mois <= 12) &&
                (heure >= 1 && heure <= 23) && (minutes >= 1 && minutes <= 59) &&
                (sec >= 1 && sec <= 59);
    }

    private boolean isValidType(String type){
        return type.startsWith("VL") || type.startsWith("2RM") ||
                type.startsWith("PL") || type.equals("Bus") || type.equals("Moto") || type.equals("Vélo");
    }

    private String adaptDate(String date ,String heure,String seconde, String centime) {
        String[] datej = date.split("/");
        return datej[0] + "," + datej[1] + "/" + datej[2] + "," + heure + ":"+seconde+":"+ centime;
    }

    private String adaptType(String type) {
        if (type.startsWith("VL")) return "VL";
        if (type.startsWith("Vélo")) return "2R";
        if (type.startsWith("PL")) return "PL";
        if (type.equals("Bus")) return "PL";
        if (type.equals("Moto")) return "2R";
        return type;
    }
}
