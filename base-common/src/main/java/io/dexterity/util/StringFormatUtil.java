package io.dexterity.util;

public class StringFormatUtil {


    public static String fillInteger(int num , int digit) {
        return String.format("%0"+digit+"d", num);
    }

    public static String fillDouble(int num , double digit) {
        return String.format("%0"+digit+"f", digit);
    }



}
