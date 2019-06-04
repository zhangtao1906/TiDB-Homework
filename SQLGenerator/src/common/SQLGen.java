package common;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SQLGen {
    public static Logger logger = Logger.getLogger(SQLGen.class);

    // 定义规则列表
    private List<String> ruleList = new ArrayList<>();

    // 定义第一行查询后半部分规则内容
    private List<String> queryExp = new ArrayList<>();

    // 定义除了第一行之外所有行的规则名：规则内容之间的映射（包括简单规则和复合规则）
    // 备注：
    // 简单规则名为单行规则开头的elem名称
    // 复合规则名为单行规则中的alt，且该alt为多个elem或者strlit组合而成
    // 例如cols: col | col ", " cols中的cols和col即为简单规则名；“col "," cols”即为复合规则名；
    private Map<String, ArrayList<String>> rulesMap = new HashMap<>();

    // 保存所有简单规则的规则名
    private Set<String> simpleRuleNameSet = new HashSet<>();

    // 保存所有复合规则的规则名
    private Set<String> complexRuleNameSet = new HashSet<>();

    // 传入规则文件名和要生成SQL的条数即可
    public void generateSQL(String fileName, int sqlCount) {
        if (!init(fileName))
            return;

        // 这里默认线程数为10
        int threadSize = (sqlCount > 10) ? 10 : 1;
        CountDownLatch latch = new CountDownLatch(threadSize);
        int times = sqlCount / threadSize;
        for (int i = 0; i < threadSize; i++) {
            Thread thread = new Thread(new multiThreadsGen(latch, times));
            thread.start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 初始化，读取规则、检测规则名和规则内容是否合法，均合法后将规则信息填入相关数据结构
    public boolean init(String fileName) {
        if (!readRules(fileName))
            return false;

        int rulesCount = ruleList.size();
        for (int i = 0; i < rulesCount; ++i) {
            String rule = ruleList.get(i);
            String[] ruleSplits = rule.split(":");
            String ruleName = ruleSplits[0].trim();

            // 若规则不是xxx:xxx的结构，抛出规则非法并结束程序
            if (ruleSplits.length != 2) {
                logger.error(String.format("the line starting with \"%s\" is illegal!", ruleName));
                return false;
            }

            // 若规则名非法，抛出规则非法并结束程序
            if (!isRuleNameLegal(ruleName)) {
                logger.error(String.format("rule name \"%s\" is illegal!", ruleName));
                return false;
            }

            // 添加简单规则名
            simpleRuleNameSet.add(ruleName);

            // 第一行与其他行的规则不同，验证合法性也分别验证
            if (ruleName.equals("query")) {
                if (!verifyQueryExp(ruleSplits[1]))
                    return false;
            }
            // 如果不是query，则进入该逻辑
            else {
                if (!verifyAlts(ruleName, ruleSplits[1]))
                    return false;
            }
        }
        return true;
    }

    // 读取规则到ruleList中
    public boolean readRules(String fileName) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(fileName)));
            String tmpString = null;
            while ((tmpString = reader.readLine()) != null) {
                if (!(tmpString.startsWith("//")))
                    ruleList.add(tmpString.trim());
            }
        } catch (IOException e) {
            logger.error("read file => " + fileName + " failed, you will get an empty list", e);
            return false;
        }
        return true;
    }

    // 创建多线程同时生成SQL
    public class multiThreadsGen implements Runnable {
        int times = 0;
        CountDownLatch latch;

        public multiThreadsGen(CountDownLatch latch, int times) {
            this.latch = latch;
            this.times = times;
        }

        // 传入query表达式
        public String getSubString(List<String> subQueryExp) {
            int queryExpLen = subQueryExp.size();
            String subExp = "";
            int subExpLen = 0;
            StringBuffer sbTmp = new StringBuffer();
            for (int i = 0; i < queryExpLen; ++i) {
                subExp = subQueryExp.get(i);
                subExpLen = subExp.length();

                // 字符串，处理后返回
                if (isStrLegal(subExp))
                    sbTmp.append(subExpLen > 2 ?
                            subExp.substring(1, subExpLen - 1).replaceAll("\\\\", "") : "");
                // 简单规则名，递归拼接
                else if (simpleRuleNameSet.contains(subExp))
                    sbTmp.append(simpleRecursion(subExp));
                // 复合规则名，递归拼接（一般query中不存在复合规则名）
                else if (complexRuleNameSet.contains(subExp))
                    sbTmp.append(complexRecursion(subExp));
                // 属于未定义的规则名，直接结束程序
                else
                {
                    logger.error(String.format("[%s] has not been defined!", subExp));
                    System.exit(-1);
                }
            }
            return sbTmp.toString();
        }

        @Override
        public void run() {
            // 该线程执行制定次数，生成制定数量条SQL
            for (int t = 1; t <= times; ++t) {
                String result = getSubString(queryExp);
                //System.out.println(result);
            }
            latch.countDown();
        }
    }

    // 简单规则名, 随机从规则列表中选择一个
    public String simpleRecursion(String subExp) {
        ArrayList<String> subElemList = rulesMap.get(subExp);
        int subElemSize = subElemList.size();

        // 多线程并发生成随机数
        int randomIdx = ThreadLocalRandom.current().nextInt(subElemSize);
        String val = subElemList.get(randomIdx);

        // 当前alt已经是strlit了，处理后返回
        if (isStrLegal(val))
            return val.length() > 2 ?
                    val.substring(1, val.length() - 1).replaceAll("\\\\", "") : "";

        // 当前alt是简单规则名，继续递归获取，直到val为strlit时终止
        if (simpleRuleNameSet.contains(val))
            return simpleRecursion(val);

        // 当前alt是复合规则名，调用复合规则函数
        if (complexRuleNameSet.contains(val))
            return complexRecursion(val);

        return val;
    }

    // 复杂规则名，从头到尾依次拼接
    public String complexRecursion(String subElemName) {
        ArrayList<String> subElemList = rulesMap.get(subElemName);
        int subElemSize = subElemList.size();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < subElemSize; ++i) {
            String tmp = subElemList.get(i);

            if (isStrLegal(tmp))
                sb.append(tmp.length() > 2 ?
                        tmp.substring(1, tmp.length() - 1).replaceAll("\\\\", "") : "");

            // 当前alt是简单规则名，继续递归获取，直到val为strlit时终止
            if (simpleRuleNameSet.contains(tmp))
                sb.append(simpleRecursion(tmp));

            // 当前alt是复合规则名，调用复合规则函数
            // 无论是否存在复合规则名嵌套复合规则名的情况，这里都支持
            if (complexRuleNameSet.contains(tmp))
                sb.append(complexRecursion(tmp));
        }
        return sb.toString();
    }

    // 将一个query子串分割成独立的部分，并放入给定的动态数组中
    public boolean sepQueryExp(String queryExpression, List<String> itemList) {
        Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
        Matcher regexMatcher = regex.matcher(queryExpression);
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                // 匹配所有双引号的串
                itemList.add("\"" + regexMatcher.group(1) + "\"");
            } else if (regexMatcher.group(2) != null) {
                // 匹配到单独用单引号分割的串，这是非法的
                logger.error("query expression can not use single-quoted string outside double-quote!");
                return false;
            } else {
                // 匹配到没有引号括住的串
                itemList.add(regexMatcher.group());
            }
        }
        return true;
    }

    // 验证query后的表达式是否合法
    public boolean verifyQueryExp(String queryExpression) {
        // 检测括号是否匹配
        if (!areQueryQuotesLegal(queryExpression)) {
            logger.error(String.format("query expression [%s] is illegal!", queryExpression));
            return false;
        }

        // 分割query表达式成单独的部分
        if (!sepQueryExp(queryExpression, queryExp))
            return false;

        return true;
//        logger.debug(queryExp);
    }

    // 验证规则后的alts是否合法
    public boolean verifyAlts(String ruleName, String altGroup) {
        String[] alts = altGroup.trim().split("\\|");
        for (int j = 0; j < alts.length; ++j)
            alts[j] = alts[j].trim();
        ArrayList<String> altsExp = new ArrayList<>(Arrays.asList(alts));

        // 验证alts的合法性
		if (!areAltsLegal(altsExp))
		{
			logger.error(String.format("\"%s\" is illegal!", altsExp));
			return false;
		}
        // 放入整行的简单规则名：整行的规则内容
        rulesMap.put(ruleName, altsExp);

        // 逐个检查当前行中是否存在复合规则
        for (int j = 0; j < alts.length; ++j) {
            // 将复合规则名：复合规则内容放入相关数据结构
            if (!isStrLegal(alts[j]) && !simpleRuleNameSet.contains(alts[j])) {
                complexRuleNameSet.add(alts[j]);
                List tmpList = new ArrayList<>();
                sepQueryExp(alts[j], tmpList);
                rulesMap.put(alts[j], (ArrayList<String>) tmpList);
            }
        }
        //logger.info(altsExp);
        return true;
    }

    // 检查strlit是否合法，必须是以双引号开头和结尾
    public boolean isStrLegal(String name) {
        return name.startsWith("\"") && name.endsWith("\"");
    }

    // 检查规则的双引号个数是否匹配
    public boolean areQueryQuotesLegal(String queryExp) {
        String[] splits = queryExp.split(" ");
        int quoteCount = 0;
        for (String str : splits) {
            int len = str.length();
            for (int i = 0; i < len; ++i) {
                if (str.charAt(i) == '"')
                    ++quoteCount;
                else if (str.charAt(i) == '\\')
                    ++i;
            }
        }
        return (quoteCount & 0x01) == 0;
    }

    // 检查简单规则的名称是否合法，规则名不能以数字开头
    public boolean isRuleNameLegal(String name) {
        return (!name.matches("^[0-9].*")) && name.matches("^[A-Za-z0-9_]+");
    }

    // 检查alts中的每一项是否为strlit和elem，对于复合规则，目前只能检测它的双引号个数是否匹配
    public boolean areAltsLegal(ArrayList<String> alts) {
        for (String alt : alts)
            if (!isRuleNameLegal(alt) && !isStrLegal(alt) && !areQueryQuotesLegal(alt))
                return false;
        return true;
    }
}