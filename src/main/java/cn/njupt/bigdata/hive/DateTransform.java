package cn.njupt.bigdata.hive;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 把格式为： "31/Aug/2015:00:04:37 +0800" 转换为： "2015-08-31 00:04:37"
 */
public class DateTransform extends UDF {
    private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Text evaluate(Text input) {
        // 过滤
        if (input == null) {
            return null;
        }

        Text output = new Text();
        String inputDate = input.toString();

        try {
            // parse
            Date parseDate = inputFormat.parse(inputDate);
            // format
            String outputDate = outputFormat.format(parseDate);
            // set
            output.set(outputDate);
//            System.out.println(outputDate);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return output;
    }
    public static void main(String[] args){
        new DateTransform().evaluate(new Text(args[0]));
    }
}