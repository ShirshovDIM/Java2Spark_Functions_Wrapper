package com.alfa;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.time.format.DateTimeFormatter;

public class MaskingFunctionsTestRunner {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("MaskingFunctionsTestRunner")
                .master("local[*]")
//                .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED "
//                        + "--add-opens=java.base/java.io=ALL-UNNAMED "
//                        + "--add-opens=java.base/java.util=ALL-UNNAMED "
//                        + "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
//                .config("spark.jars", "C:\\Users\\dbezu\\Desktop\\masking-functions\\target\\masking-functions-1.0-SNAPSHOT.jar")
                .getOrCreate();

        // Тут регистрация udf
        MaskingFunctionsWrapper.registerAllUDFs(spark);

        List<Row> testData = Arrays.asList(
                // email
                RowFactory.create("hello@worlds.ru", null, "email"),
                // first_letter_first_word
                RowFactory.create("Johnny Silverhand", null, "first_letter_first_word"),
                // replace_day_in_date
                RowFactory.create(null, Date.valueOf(LocalDate.of(2025, 5, 14)),"14.05.2024"),
                // replace_inside
                RowFactory.create("1234-5678-9012", null, "****-****-****"),
                // crop_value_to
                RowFactory.create("VerySensitiveData123", null,"Data")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("data", DataTypes.StringType, false, Metadata.empty()),
                new StructField("date_data", DataTypes.DateType, true, Metadata.empty()),
                new StructField("pattern", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(testData, schema);
        df.createOrReplaceTempView("test_data");

        System.out.println("=== Исходник ===");
        df.show(false);

        System.out.println("\n=== Результат маскирования ===");

        spark.sql(
                "SELECT data, pattern, email(data, pattern) AS masked_email " +
                        "FROM test_data WHERE pattern = 'email'"
        ).show(false);

        spark.sql(
                "SELECT data, pattern, first_letter_first_word(data, pattern) AS masked_name " +
                        "FROM test_data WHERE pattern = 'first_letter_first_word'"
        ).show(false);

        spark.sql(
                "SELECT date_data, pattern, replace_day_in_date(date_data, '14.05.2024') AS masked_date " +
                        "FROM test_data WHERE pattern = '14.05.2024'"
        ).show(false);

        spark.sql(
                "SELECT data, pattern, replace_inside(data, 'dd**-*dd*-**dd') AS masked_card " +
                        "FROM test_data WHERE pattern = '****-****-****'"
        ).show(false);

        spark.sql(
                "SELECT data, pattern, crop_value_to(data, 'Data') AS cropped_value " +
                        "FROM test_data WHERE pattern = 'Data'"
        ).show(false);

        spark.stop();
    }
}