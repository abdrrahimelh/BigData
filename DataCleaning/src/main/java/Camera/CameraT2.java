package Camera;

import Helper.Helper;
import Prototype.DataCleaner;

public class CameraT2 implements DataCleaner {
    @Override
    public boolean isValid(String str) {
        String[] tokens = str.split(",");
        //if (tokens.length != 6)
        //    return false;
        try {
            if (!isValidCategory(tokens[1])) return false;
            String[] horodate = tokens[2].split(" ");
            if (!isValidDate(horodate[0], horodate[1])) return false;

        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public String adapt(String line, String fileName) {
        String[] tokens = line.split(",");
        String str = "";
        str += Helper.getPosition(fileName);
        str+=",";
        str += "Camera,";
        str += adaptSens(tokens[3], tokens[4]) + ",";
        String[] horodate = tokens[2].split(" ");
        str += adaptDate(horodate[0], horodate[1]) + ",";
        str += "0,";
        str += adaptType(tokens[1]) ;

        return str;
    }
    private boolean isValidDate(String date, String time) {
        String[] timewithoutc = time.split("\\.");
        String[] timetoken = timewithoutc[0].split(":");
        String[] datetoken = date.split("/");
        int jour = Integer.parseInt(datetoken[2]);
        int mois = Integer.parseInt(datetoken[1]);
        if (jour > 31 || jour <= 0) return false;
        if (mois > 12 || mois <= 0) return false;
        int hour = Integer.parseInt(timetoken[0]);
        int minute = Integer.parseInt(timetoken[1]);
        int second = Integer.parseInt(timetoken[2]);
        if (hour >= 24 || hour < 0) return false;
        if (minute >= 60 || minute < 0) return false;
        if (second >= 60 || second < 0) return false;
        return true;
    }

    private boolean isValidCategory(String ser) {
        if (ser.startsWith("PL")) return true;
        if (ser.equals("MOTO") || ser.equals("BUS") || ser.equals("VELO") || ser.equals("VL") || ser.equals("UT")|| ser.equals("EDPM")) return true;
        return false;
    }

    private String adaptType(String type) {
        if (type.startsWith("PL")|| type.equals("BUS")) return "PL";
        if (type.equals("MOTO") || type.equals("VELO")) return "2R";
        if (type.equals("EDPM")|| type.equals("UT")) return "VL";
        return type;
    }

    private String adaptSens(String direction1, String direction2) {
        if (direction2.startsWith("S")) return "1";
        else return "2";
    }

    private String adaptDate(String date, String time) {
        String[] timewithoutc = time.split("\\.");
        String[] timetoken = timewithoutc[0].split(":");
        String[] datetoken = date.split("/");
        return datetoken[2] + "," + datetoken[1] + "/" + datetoken[0] + "," + timewithoutc[0]+":"+ timewithoutc[1];
    }
}
