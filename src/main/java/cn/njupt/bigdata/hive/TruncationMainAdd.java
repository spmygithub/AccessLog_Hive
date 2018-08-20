package cn.njupt.bigdata.hive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
/**
 * 从"http://www.baidu.com/user.php?act=mycourse"提取主地址，即"http://www.baidu.com"
 */
public class TruncationMainAdd extends UDF {

    public Text evaluate(Text add) {
        // 过滤为null的输入
        if (add == null) {
            return null;
        }

        String address = add.toString();
        // 过滤不是http://开头的输入
        if (!address.startsWith("http://")) {
            return null;
        }
        // 模式匹配
        Pattern p = Pattern.compile("http://[^/]+(/\\S*)");
        Matcher m = p.matcher(address);
        // 获取分组 即 /user.php?act=mycourse
        String s = null;
        if (m.find()) {
            s = m.group(1);
        }
        // 索引
        int index = address.lastIndexOf(s);
        // 截取
        address = address.substring(0, index);

        // 结果
        Text result = new Text();
        // 构造结果
        result.set(address);
//        System.out.println(result);
        return result;
    }
    public static void main(String[] args){
        new TruncationMainAdd().evaluate(new Text(args[0]));
    }
}
