package common;

import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SQLGen {
	public static Logger logger = Logger.getLogger(SQLGen.class);

	// 定义规则列表
	private List<String> ruleList = new ArrayList<String>();

	// 定义第一行查询后半部分规则内容
	private List<String> queryExp = new ArrayList<>();

	//定义除了第一行之外所有行的规则名：规则内容之间的映射
	private Map<String, ArrayList<String>> rulesMap = new HashMap<>();

	// 保存所有规则的规则名
	private Set<String> ruleNames = rulesMap.keySet();

	// 定义生成随机数的类
	Random randSeed = new Random();

	// 传入规则文件名和要生成SQL的条数即可
	public void generateSQL(String fileName, int sqlCount)
	{
		boolean verifyRst = init(fileName);
		if (!verifyRst)
			return;

		// 这里默认线程数为10
		int threadSize = (sqlCount > 10) ? 10 : 1;
		CountDownLatch latch = new CountDownLatch(threadSize);
		int times = sqlCount / threadSize;
		for (int i = 0; i < threadSize; i++)
		{
			Thread thread = new Thread(new multiGen(latch, times));
			thread.start();
			try {
				Thread.sleep(100);
				if (thread.isInterrupted())
					return;
			}catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		try{
			latch.await();
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

	// 初始化，读取规则、检测规则名和规则内容是否合法，均合法后将规则信息填入相关数据结构
	public boolean init(String fileName)
	{
		if (!readRules(fileName))
			return false;

		int len = ruleList.size();
		for (int i = 0; i < len; ++i)
		{
			String rule = ruleList.get(i);
			String [] ruleSplits = rule.split(":");
			String ruleName = ruleSplits[0].trim();

			// 若规则不是xxx:xxx的结构，抛出规则非法并结束程序
			if (ruleSplits.length != 2) {
				logger.error(String.format("the line starting with \"%s\" is illegal!", ruleName));
				return false;
			}

			// 若规则名非法，抛出规则非法并结束程序
			if (!isRuleNameLegal(ruleName))
			{
				logger.error(String.format("rule name \"%s\" is illegal!", ruleName));
				return false;
			}

			// 第一行与其他行的规则不同，验证合法性也分别验证
			if (ruleName.equals("query"))
			{
				if (!verifyQueryExp(ruleSplits[1]))
					return false;
			}
			// 如果不是query，则进入该逻辑
			else
			{
				if (!verifyAlts(ruleName, ruleSplits[1]))
					return false;
			}
		}
		return true;
	}

	// 读取规则到ruleList中
	public boolean readRules(String fileName)
	{
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(fileName)));
			String tmpString = null;
			while ((tmpString = reader.readLine()) != null ) {
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
	public class multiGen implements Runnable{
		int times = 0;
		CountDownLatch latch;
		public multiGen(CountDownLatch latch, int times)
		{
			this.latch = latch;
			this.times = times;
		}

		@Override
		public void run() {
			int queryExpLen = queryExp.size();
			String subExp = null;
			int subExpLen = 0;
			// 该线程执行制定次数，生成制定数量条SQL
			for (int t = 1; t <= times; ++ t) {
				StringBuilder sb = new StringBuilder();
				// 逐个遍历query中的表达式
				for (int i = 0; i < queryExpLen; ++i) {
					subExp = queryExp.get(i);
					subExpLen = subExp.length();
					if (isStrLegal(subExp))
					{
						if (subExpLen == 2)
							sb.append("");
						else
							sb.append(subExp.substring(1, subExpLen - 1));

					}
					else if (ruleNames.contains(subExp))
						sb.append(recursion(subExp));
					else
					{
						logger.error(String.format("Illegal subExp \"%s\" in query!", subExp));
						Thread.currentThread().interrupt();
					}
				}
				System.out.println(sb.toString());
			}
			latch.countDown();
		}
	}



	// 如果获取的是elem而不是strlit，则递归获取该elem下的值，且还可能会取到elem，所以用递归的方式可满足任意极端情况
	public  String recursion(String subExp)
	{
		// 获取该规则名对应的规则内容
		ArrayList<String> elemExp = rulesMap.get(subExp);
		int expLen = elemExp.size();
		int random = randSeed.nextInt(expLen);
		String val = elemExp.get(random);

		// 如果这里取到的值还是elem，则继续递归获取，直到val为strlit时才终止
		if (ruleNames.contains(val))
			return recursion(val);
		if (val.length() > 2)
			return val.substring(1, val.length() - 1).replaceAll("\\\\", "");
		else
			return "";
	}


	// 验证query后的表达式是否合法
	public boolean verifyQueryExp(String queryExpression)
	{
		// 需要检测括号是否匹配
		if (!areQueryExpLeagal(queryExpression))
		{
			logger.error(String.format("query expression [%s] is illegal!", queryExpression));
			return false;
		}

		Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
		Matcher regexMatcher = regex.matcher(queryExpression);
		while (regexMatcher.find()) {
			if (regexMatcher.group(1) != null) {
				// 这里匹配所有双引号的串
				queryExp.add("\"" + regexMatcher.group(1) + "\"");
			} else if (regexMatcher.group(2) != null) {
				// 匹配到单独用单引号分割的串，这是非法的
				logger.error("query expression can not use single-quoted string outside double-quote!");
				return false;
			} else {
				// 匹配到没有引号括住的串
				queryExp.add(regexMatcher.group());
			}
		}
//				logger.debug(queryExp);
		return true;
	}

	// 验证规则后的alts是否合法
	public boolean verifyAlts(String ruleName, String altGroup)
	{
		String[] alts = altGroup.trim().split("\\|");
		for (int j = 0; j < alts.length; ++j)
			alts[j] = alts[j].trim();
		ArrayList<String> altsExp = new ArrayList<String>(Arrays.asList(alts));
		if (!areAltsLegal(altsExp))
		{
			logger.error(String.format("\"%s\" is illegal!", altsExp));
			return false;
		}
		rulesMap.put(ruleName, altsExp);
		//logger.debug(altsExp);
		return true;
	}

	// 检查strlit是否合法
	public boolean isStrLegal(String name)
	{
		return (name.startsWith("\"") && name.endsWith("\""));
	}

	// 检查query的内容是否合法
	public boolean areQueryExpLeagal(String queryExp)
	{
		String[] splits = queryExp.split(" ");
		int quoteCount = 0;
		for (String str : splits)
		{
			int len = str.length();
			for (int i = 0; i < len; ++i)
			{
				if (str.charAt(i) == '"')
					++quoteCount;
				else if (str.charAt(i) == '\\')
					++i;
			}
		}
		return (quoteCount & 0x01) == 0;
	}

	// 检查规则的名称是否合法，规则名不能以数字开头
	public boolean isRuleNameLegal(String name)
	{
		return (!name.matches("^[0-9].*")) && name.matches("^[A-Za-z0-9_]+");
	}

	// 检查Alt是否是strlit和elem两者之一
	public boolean areAltsLegal(ArrayList<String> alts)
	{
		for (String alt : alts)
			if (!isRuleNameLegal(alt) && !isStrLegal(alt))
				return false;
		return true;
	}
}