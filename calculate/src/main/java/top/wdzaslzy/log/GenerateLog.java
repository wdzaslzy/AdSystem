package top.wdzaslzy.log;

import java.io.FileWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.UUID;

/**
 * @author lizy
 **/
public class GenerateLog {

    private static List<String> ipList = new ArrayList<String>();
    private static Map<String, List<String>> provinceMap = new HashMap<String, List<String>>();
    private static List<String> provinceList = new ArrayList<String>();

    public static void defIpList() {
        ipList.add("113.52.166.0/24");
        ipList.add("113.52.168.0/21");
        ipList.add("113.52.176.0/20");
        ipList.add("113.52.228.0/22");
        ipList.add("113.54.0.0/15");
        ipList.add("113.56.0.0/15");
        ipList.add("113.58.0.0/16");
        ipList.add("113.59.0.0/17");
        ipList.add("113.59.224.0/22");
        ipList.add("113.62.0.0/15");
        ipList.add("113.64.0.0/10");
        ipList.add("113.128.0.0/15");
        ipList.add("113.130.96.0/20");
        ipList.add("113.130.112.0/21");
        ipList.add("113.132.0.0/14");
        ipList.add("113.136.0.0/13");
        ipList.add("113.194.0.0/15");
        ipList.add("113.197.100.0/23");
        ipList.add("113.197.102.0/24");
        ipList.add("113.200.0.0/15");
        ipList.add("113.202.0.0/16");
        ipList.add("113.204.0.0/14");
        ipList.add("113.208.96.0/19");
        ipList.add("113.208.128.0/17");
        ipList.add("202.14.236.0/22");
        ipList.add("202.14.246.0/24");
        ipList.add("202.14.251.0/24");
        ipList.add("202.20.66.0/24");
        ipList.add("202.20.79.0/24");
        ipList.add("202.20.87.0/24");
        ipList.add("202.20.88.0/23");
        ipList.add("202.20.90.0/24");
        ipList.add("202.20.94.0/23");
        ipList.add("202.20.114.0/24");
        ipList.add("202.20.117.0/24");
        ipList.add("202.20.120.0/24");
        ipList.add("202.20.125.0/24");
        ipList.add("202.20.126.0/23");
        ipList.add("202.21.48.0/20");
        ipList.add("202.21.131.0/24");
        ipList.add("202.21.132.0/24");
        ipList.add("202.21.141.0/24");
        ipList.add("202.21.142.0/24");
        ipList.add("202.21.147.0/24");
        ipList.add("202.21.148.0/24");
        ipList.add("202.21.150.0/23");
        ipList.add("202.21.152.0/23");
        ipList.add("202.21.154.0/24");
    }

    public static void defProvinceList() {
        List<String> beijing = new ArrayList<String>();
        beijing.add("北京");
        List<String> tianjin = new ArrayList<String>();
        tianjin.add("天津");
        List<String> shanghai = new ArrayList<String>();
        shanghai.add("上海");
        List<String> chongqing = new ArrayList<String>();
        chongqing.add("重庆");
        List<String> xinjiang = new ArrayList<String>();
        xinjiang.add("乌鲁木齐");
        List<String> ningxia = new ArrayList<String>();
        ningxia.add("银川");
        ningxia.add("中卫");
        List<String> shandong = new ArrayList<String>();
        shandong.add("济南");
        shandong.add("青岛");
        shandong.add("潍坊");
        List<String> heilongjiang = new ArrayList<String>();
        heilongjiang.add("哈尔滨");
        List<String> lioaning = new ArrayList<String>();
        lioaning.add("长春");
        lioaning.add("大连");
        lioaning.add("沈阳");
        List<String> jiangsu = new ArrayList<String>();
        jiangsu.add("南京");
        jiangsu.add("苏州");
        jiangsu.add("徐州");
        jiangsu.add("连云港");
        jiangsu.add("无锡");
        List<String> anhui = new ArrayList<String>();
        anhui.add("合肥");
        anhui.add("芜湖");
        anhui.add("蚌埠");
        provinceMap.put("北京", beijing);
        provinceMap.put("天津", tianjin);
        provinceMap.put("上海", shanghai);
        provinceMap.put("重庆", chongqing);
        provinceMap.put("新疆", xinjiang);
        provinceMap.put("宁夏省", ningxia);
        provinceMap.put("山东省", shandong);
        provinceMap.put("黑龙江省", heilongjiang);
        provinceMap.put("辽宁省", lioaning);
        provinceMap.put("江苏省", jiangsu);
        provinceMap.put("安徽省", anhui);

        provinceList.addAll(provinceMap.keySet());
    }

    public static String buildLog() throws ParseException {
        String sessionId = UUID.randomUUID().toString().replace("-", "");       //会话标识
        int advertisersId = (int) ((Math.random() + 1) * 100);                    //广告主ID
        int advertisementId = (int) ((Math.random() + 100) * 1000);             //广告id
        int putInType = (int) ((Math.random() * 2) + 1);                        //点击类型
        String adPrice = String.format("%.2f", Math.random() * 10);             //广告价格
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String requestTime = null;
        Long startTime = dateFormat.parse("2018-01-01 00:00:00").getTime();
        Long endTime = dateFormat.parse("2018-12-31 23:59:59").getTime();
        Date theDate = new Date(startTime + (long) (Math.random() * (endTime - startTime)));
        requestTime = dateFormat.format(theDate);                   //点击时间
        int ipRandom = (int) (Math.random() * ipList.size());
        String requestIp = ipList.get(ipRandom);                    //ip地址
        int index = (int) (Math.random() * provinceMap.size());
        String province = provinceList.get(index);                  //省份
        int cityIndex = (int) (Math.random() * provinceMap.get(province).size());
        String city = provinceMap.get(province).get(cityIndex);     //城市

        return sessionId + "|" + advertisersId + "|" + advertisementId + "|"
                + putInType + "|" + adPrice + "|" + requestTime + "|" + requestIp + "|"
                + city + "|" + province + "|" + (int) ((Math.random() * 2) + 1);
    }

    public static void main(String[] args) {
        defIpList();
        defProvinceList();
        try {
            ResourceBundle bundle = ResourceBundle.getBundle("common");
            String ad_click_log_catalog = bundle.getString("ad_click_log_catalog");
            FileWriter fileWriter = new FileWriter(ad_click_log_catalog, true);

            int index = 0;
            while (index < 10000000) {
                fileWriter.write(buildLog() + "\r\n");
                fileWriter.flush();
                index++;
            }
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
