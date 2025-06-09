package ru.dimker;

import java.time.LocalDate;
import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MaskingFunctions {
    private static String replaceInside(String data, String pattern) {
        try {
            data = data.trim();
            
            if (data.length() < pattern.length()) {
                throw new Exception();
            }
            
            char defaultSymbol = '*';
            List<Integer> indexList = new ArrayList<>();
            for (int idx = 0; idx < pattern.length(); idx++) {
                if (pattern.charAt(idx) == defaultSymbol) {
                    indexList.add(idx);
                }
            }
            int slen = pattern.length();
            String[] splitSymbol = data.split("(?<!^)");
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < slen; i++) {
                if (indexList.contains(i)) {
                    result.append(defaultSymbol);
                } else {
                    result.append(splitSymbol[i]);
                }
            }
            return result.toString();
        }
        catch (Exception e) {
            return replaceAll(data, "*");
        }
    }

    private static String replaceThreeAsterisks(String data, String pattern) {
        return pattern;
    }

    private static String firstLetterWord(String data, String pattern) {
        try {
            data = data.trim();

            if ("d.e".equals(pattern)) {
                return data.charAt(0) + ".";
            }

            String[] words = data.split(" ");
            List<String> result = new ArrayList<>();
            
            for (int w = 0; w < Math.min(words.length, 3); w++) {
                String word = words[w];
                StringBuilder maskedWord = new StringBuilder();
                maskedWord.append(word.charAt(0)).append('.');
                
                for (int i = 0; i < word.length(); i++) {
                    char ch = word.charAt(i);
                    if ("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~".indexOf(ch) != -1) {
                        if (i + 1 < word.length()) {
                            maskedWord.append(ch).append(word.charAt(i + 1)).append('.');
                        }
                    }
                }
                
                result.add(maskedWord.toString());
            }
            
            return String.join(" ", result);
        }
        catch (Exception e) {
            return replaceAll(data, "*");
        }
    }

    private static String noMasking(String data, String pattern) {
        return data;
    }

    private static String replaceAll(String data, String pattern) {
        data = data.trim();
        String regex = "[^" + Pattern.quote("!\"#$%&'() +,-./:;<=>?@[\\]^_`{|}~") + "]";
        return data.replaceAll(regex, pattern);
    }

    private static String email(String data, String pattern) {
        try {
            data = data.trim();
            String[] splitSymbol = data.split("@");
            String localPart = splitSymbol[0];
            String domainPart = splitSymbol[1];
            String[] domainSplit = domainPart.split("\\.");
            String highDomain = domainSplit[domainSplit.length - 1];
            String firstPart = localPart.length() > 2 ? localPart.substring(0, 2) : localPart;
            return firstPart + "****@****." + highDomain;
        }
        catch (Exception e) {
            return "****@****";
        }
    }

    private static String firstLetterFirstWord(String data, String pattern) {
        try {
            data = data.trim();
            String[] splitStr = data.split(" ");
            String[] firstPart = splitStr[0].split("-");
            String firstName = Arrays.stream(firstPart)
                    .map(part -> part.charAt(0) + ".")
                    .collect(Collectors.joining("-"));
            String remainingWords = splitStr.length > 1 ? String.join(" ", Arrays.copyOfRange(splitStr, 1, splitStr.length))
                    : "";
            return firstName + (remainingWords.isEmpty() ? "" : " " + remainingWords);
        }
        catch (Exception e) {
            return replaceAll(data, "*");
        }
    }

    private static String replaceDayInDate(LocalDate date, String pattern) {
        String[] parts = pattern.split("\\.");
        try {
            int day = Integer.parseInt(parts[0]);
            date = date.withDayOfMonth(day);
        }
        catch (Exception e) {
        }

        try {
            int month = Integer.parseInt(parts[1]);
            date = date.withMonth(month);
        }
        catch (Exception e) {
        }

        try {
            int year = Integer.parseInt(parts[2]);
            date = date.withYear(year);
        }
        catch (Exception e) {
        }
        
        return date.toString();
    }

    private static String cropValueTo(String data, String pattern) {
        data = data.trim();
        int index = data.indexOf(pattern);
        try {
            if (index != -1) {
                return data.substring(0, index);
            } else {
                return "***";
            }
        }
        catch (Exception e) {
            return "***";
        }
    }

    private static String replaceInsideDynamic(String data, String pattern) {
        try {
            data = data.trim();

            if (data.length() < pattern.length()) {
                throw new Exception();
            }
            String startNoMask = "", endNoMask = "", mask = "";
            char maskChar = '?';

            for (int i = 0; i < pattern.length(); i++) {
                if (pattern.charAt(i) == 'd') {
                    startNoMask += data.charAt(i);
                } else {
                    maskChar = pattern.charAt(i);
                    break;
                }
            }
            for (int i = 0; i < pattern.length(); i++) {
                if (pattern.charAt(pattern.length() - 1 - i) == 'd') {
                    endNoMask += data.charAt(data.length() - 1 - i);
                } else {
                    break;
                }
            }
            endNoMask = new StringBuilder(endNoMask).reverse().toString();

            int maskLength = data.length() - startNoMask.length() - endNoMask.length();

            if (maskLength == 0) {
                throw new Exception();
            }

            for (int i = 0; i < maskLength; i++) {
                mask += maskChar;
            }

            return startNoMask + mask + endNoMask;
        }
        catch (Exception e) {
            return replaceAll(data, "*");
        }
    }

    public static final Map<String, BiFunction<Object, String, String>> maskingFunctions = new HashMap<>();

    static {
        maskingFunctions.put("replace_inside", (data, pattern) -> replaceInside((String) data, pattern));
        maskingFunctions.put("replace_three_asterisks",
                (data, pattern) -> replaceThreeAsterisks((String) data, pattern));
        maskingFunctions.put("first_letter_word", (data, pattern) -> firstLetterWord((String) data, pattern));
        maskingFunctions.put("no_masking", (data, pattern) -> noMasking((String) data, pattern));
        maskingFunctions.put("replace_all", (data, pattern) -> replaceAll((String) data, pattern));
        maskingFunctions.put("email", (data, pattern) -> email((String) data, pattern));
        maskingFunctions.put("first_letter_first_word",
                (data, pattern) -> firstLetterFirstWord((String) data, pattern));
        maskingFunctions.put("replace_day_in_date", (data, pattern) -> replaceDayInDate((LocalDate) data, pattern));
        maskingFunctions.put("crop_value_to", (data, pattern) -> cropValueTo((String) data, pattern));
        maskingFunctions.put("replace_inside_dynamic", (data, pattern) -> replaceInsideDynamic((String) data, pattern));
    }

    // Пример использования
    //public static void main(String[] args) {
    //     System.err.println(maskingFunctions
    //         .get("replace_inside")
    //         .apply(" 12w2", "dd*dd"));
    //}
}