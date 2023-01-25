package Radar;

import Helper.Helper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RadarViking implements Cleaner {
    @Override
    public boolean isValid(String str) {
        String[] tokens = str.split(",");
        if (tokens.length != 7)
            return false;
        try {
            if(!isValidDirection(tokens[0])) return false;
            if(!isValidDate(tokens[1], tokens[2], tokens[3])) return false;
            if(!isValidCategory(tokens[5], tokens[6])) return false;
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
        str += "RADAR_VIKING";
        str += ",";
        str += adaptDirection(tokens[0]) + ",";
        str += tokens[1] + ",";
        str += ",";
        int h = Integer.parseInt(tokens[2]);
        int s = Integer.parseInt(tokens[3]);
        int hh = h/100;
        int m = h%100;
        int ss = s/100;
        int c = s%100;
        str += hh + ":" + m + ":" + ss + ":" + c + ",";
        str += tokens[4].substring(2) + ",";
        str += tokens[6];
        return str;
    }

    private static boolean isValidDate(String day,  String hour, String sec){
        int jour = Integer.parseInt(day);
        if(jour > 31 || jour <= 0) return false;
        if( hour.length() > 4 ) return false;
        if( sec.length() > 4 ) return false;
        int h = Integer.parseInt(hour);
        int hh = h/100;
        int m = h%100;
        if(hh>=24 || hh<0) return false;
        if(m>=60 || m<0) return false;
        int s = Integer.parseInt(sec);
        int ss = s/100;
        if(ss>=60 || ss<0) return false;
        return true;
    }

    private static boolean isValidCategory(String ser, String type){
        if(ser.length() != 6) return false;
        int size = Integer.parseInt(ser.substring(2,6));
        if(!( type.equals("2R") || type.equals("VL") || type.equals("PL"))) return false;
        if(size <205 && !type.equals("2R")) return false;
        if(  size >= 205 && size < 1140 && !type.equals("VL")) return false;
        if(size >= 1140 && !type.equals("PL")) return false;
        return true;
    }

    private static boolean isValidDirection(String direction){
        int sens = Integer.parseInt(direction);
        return sens == 1 || sens == 2;
    }

    private String adaptDirection(String direction) {
        if (direction.equals("Sortie fac")) return "2";
        if (direction.equals("Entrée fac")) return "1";
        return direction;
    }
}