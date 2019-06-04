package main;

import common.SQLGen;
import org.apache.log4j.Logger;

public class Main {
    public static Logger logger = Logger.getLogger(SQLGen.class);
    public static void main(String[] args) {
        SQLGen sg = new SQLGen();
        long startTime=System.currentTimeMillis();
        sg.generateSQL("conf\\complexRule.case", 20000000);
        long endTime=System.currentTimeMillis();
        logger.info(String.format("程序运行时间：%d ms",(endTime - startTime)));
    }
}
