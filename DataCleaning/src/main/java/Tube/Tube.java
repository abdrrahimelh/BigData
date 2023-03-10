package Tube;

import Helper.Helper;

import Prototype.DataCleaner;

public class Tube implements DataCleaner {
    @Override
    public boolean isValid(String str) {
        String[] tokens = str.split(",");
        //if (tokens.length != 7)
        //    return false;
        try {
            if(!isValidDate(tokens[0], tokens[1], tokens[2], tokens[3])) return false;
            if(!isValidCategory(tokens[5])) return false;
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
        str += "TUBE_MIXTRA";
        str += ",";
        str += Helper.getDirectionsTube(fileName);
        str += ",";
        str+= adaptDate(tokens[0], tokens[1], tokens[2], tokens[3]) + ",";
        str += tokens[4] + ",";
        str += adaptType(tokens[5]);
        return str;
    }

    private static boolean isValidDate(String date, String hour, String sec, String milli){
        int j = Integer.parseInt(date.split("/")[0]);
        int mo = Integer.parseInt(date.split("/")[1]);
        int y = Integer.parseInt(date.split("/")[2]);
        int h = Integer.parseInt(hour.split(":")[0]);
        int m = Integer.parseInt(hour.split(":")[1]);
        int s = Integer.parseInt(sec);
        int ms = Integer.parseInt(milli);
        return j > 0 && j <= 31 && mo >= 1 && mo <= 12 && h >= 0 && h <= 23 && m >= 0 && m < 60 && s >= 0 && s < 60 && ms >= 0 && ms < 99;
    }

    private static boolean isValidCategory(String type){
        return type.startsWith("VL") || type.startsWith("2RM") ||  type.startsWith("2R") ||
        type.startsWith("PL") || type.equals("Bus") || type.equals("Deux roues");
    }

    private static String adaptDate(String date, String hour, String sec, String milli){
        String str = "";
        str += date.substring(0, date.indexOf('/')) + ","; //JOUR
        str += date.substring(date.indexOf('/') + 1) + ","; //MOIS/ANNEE
        str += hour + ":" + sec + ":" + milli; //H:M:S:MS
        return str;
    }

    private static String adaptType(String type) {
        if (type.startsWith("VL")) return "VL";
        if (type.startsWith("2RM") || type.startsWith("2R")) return "2R";
        if (type.startsWith("PL")) return "PL";
        if (type.equals("Bus")) return "PL";
        if (type.equals("Deux roues")) return "2RM";
        return type;
    }
}
