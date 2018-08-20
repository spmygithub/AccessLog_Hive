package cn.njupt.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 从 "GET /course/view.php?id=27 HTTP/1.1" 中获取请求地址，即 /course/view.php?id=27
 */
public class TruncationRequestAddr extends UDF {

    public Text evaluate(Text add) {
        // 过滤
        if (add == null) {
            return null;
        }
        // 按照空格分割
        String[] strings = add.toString().split(" ");
        // 过滤分割后长度小于3的字符
        if (strings.length < 3) {
            return null;
        }
        // 设置返回的结果
        Text result = new Text(strings[1]);
        System.out.println(result);
        return result;
    }

    public static void main(String[] args){
        new TruncationRequestAddr().evaluate(new Text(args[0]));
    }
}

