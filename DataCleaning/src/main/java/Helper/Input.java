package Helper;

public class Input {
    public static String inputString = "P1,P1.csv\nP10,P10.csv\nP12,P12.csv\nP13,P13.csv\nP20,P20.csv\nP3,Mixtra_Sortie_Fac_1.csv\nP3,Mixtra_Sortie_Fac_2.csv\nP3,Mixtra_Sortie_Fac_3.csv\nP9,P9_Vers_Fac_1.csv\nP9,P9_Vers_Fac_2.csv\nP9,P9_Vers_Talence_1.csv\nP9,P9_Vers_Talence_2.csv\nP19,P19_Entree.csv\nP19,P19_Sortie.csv\nP23,P23_Vers_BEC_1.csv\nP23,P23_Vers_BEC_2.csv\nP23,P23_Vers_COSEC.csv\nP24,P24_Vers_Fac.csv\nP24,P24_Vers_Rocade.csv\nP26,P26_Vers_Fac_1.csv\nP26,P26_Vers_Fac_2.csv\nP26,P26_Vers_Fac_3.csv\nP26,P26_Vers_Rocade_1.csv\nP26,P26_Vers_Rocade_2.csv\nP4,P4_Sortie_Fac.csv\nP5,P5.csv\nP17,P17_Vers_Avenue_Schweitzer.csv\nP17,P17_Vers_P16.csv";
    public static String replacedString = inputString.replaceAll(",", "/");
    public static String[] strings = replacedString.split("\n");

}
