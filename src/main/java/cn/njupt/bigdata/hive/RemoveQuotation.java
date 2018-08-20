package cn.njupt.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 *  去除数据字段两端的双引号
 */
public class RemoveQuotation extends UDF {
    public Text evaluate(Text string) {
        // 过滤
        if (null == string) {
            return null;
        }
        // 用来保存最后结果
        Text result;
        // 替换字符串的双引号为空
        String s = string.toString().replaceAll("\"", "");
        // 用中间结果生成返回值
        result = new Text(s);
//        System.out.println(result);
        return result;
    }
    public static void main(String[] args){
        new RemoveQuotation().evaluate(new Text(args[0]));
    }
}

