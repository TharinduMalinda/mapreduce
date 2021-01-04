package com.iit.bdp.mapreduce.util;

import java.util.HashMap;
import java.util.Map;

public class Util {

    public static Map<String, String> match_result(String csv) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String valueString = csv.toString();
            String[] match_result_data = valueString.split(",");
            if (match_result_data.length == 9) {
                if (match_result_data[3].toString().equals(match_result_data[4].toString())) {
                    map.put("result", "DRAW");
                } else {
                    map.put("result", "WIN");
                }
            }
            ;

        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(csv);
        }

        return map;
    }

    public static Map<String, String> transRegeion(String csv) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String valueString = csv.toString();
            String[] SingleCountryData = valueString.split(",");
            if (SingleCountryData.length == 9) {
                map.put("city", SingleCountryData[6].toString());
            }

        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(csv);
        }

        return map;
    }
}
