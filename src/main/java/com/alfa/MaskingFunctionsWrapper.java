package com.alfa;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Map;
import java.util.function.BiFunction;
import com.alfa.MaskingFunctions;


public class MaskingFunctionsWrapper {

    public static void registerAllUDFs(SparkSession spark) {
        for (Map.Entry<String, java.util.function.BiFunction<Object, String, String>> entry
                : MaskingFunctions.maskingFunctions.entrySet()) {

            final String functionName = entry.getKey();

            if (functionName.equals("replace_day_in_date")) {
                spark.udf().register(functionName, (UDF2<Date, String, String>) (date, pattern) -> {
                    if (date == null) return null;
                    LocalDate localDate = date.toLocalDate();
                    return MaskingFunctions.maskingFunctions.get(functionName).apply(localDate, pattern);
                }, DataTypes.StringType);
            } else {

                spark.udf().register(functionName, new UDF2<Object, String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String call(Object data, String pattern) {
                        Object arg = data;
                        if (data instanceof Date) {
                            arg = ((Date) data).toLocalDate();
                        }
                        return MaskingFunctions.maskingFunctions
                                .get(functionName)
                                .apply(arg, pattern);
                    }
                }, DataTypes.StringType);
            }
        }
    }
}
