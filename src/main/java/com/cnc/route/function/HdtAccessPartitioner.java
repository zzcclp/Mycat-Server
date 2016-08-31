/**
 *
 * @author zhangzc
 * Jul 4, 2016 3:46:00 PM
 * HdtAccessPartitioner.java
 * TODO : 
 *
 */
package com.cnc.route.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;
import org.opencloudb.config.model.rule.RuleAlgorithm;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

/**
 *
 * @author zhangzc
 * Jul 4, 2016 3:46:00 PM
 * HdtAccessPartitioner.java
 * TODO : 
 *
 */
public class HdtAccessPartitioner extends AbstractPartitionAlgorithm implements
RuleAlgorithm {

    private static final Logger LOGGER = Logger
            .getLogger(HdtAccessPartitioner.class);

    private Integer partitionMon;
    private Integer partitionDay;
    private String dateFormat;
    
    private Integer intervalDay;
    private Integer totalPartitions;

    private static final String DATETIME_FORMAT = "yyyy-MM-dd";

    @Override
    public void init() {
        if (dateFormat == null){
            dateFormat = DATETIME_FORMAT;
        }
        
        intervalDay = 30 / partitionDay;
        totalPartitions = partitionMon * partitionDay;
    }

    /* (non-Javadoc)
     * @see org.opencloudb.config.model.rule.RuleAlgorithm#calculate(java.lang.String)
     */
    @Override
    public Integer calculate(String columnValue) {
        // TODO Auto-generated method stub
        try {
            Date targetTime = new SimpleDateFormat(dateFormat).parse(columnValue);
            Calendar ca = Calendar.getInstance();
            ca.setTime(targetTime);

            Integer mon = ca.get(Calendar.MONTH);
            Integer day = ca.get(Calendar.DAY_OF_MONTH);

            if (day > 30) {
                day = 30;
            }
            day = day - 1; // start from 0

            int targetPartition = mon % partitionMon * partitionDay + day / intervalDay;
            return targetPartition;

        } catch (ParseException e) {
            throw new java.lang.IllegalArgumentException(e);
        }
    }

    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        // TODO Auto-generated method stub
        try {
            Integer[] re = null;
            
            Date b = new SimpleDateFormat(dateFormat).parse(beginValue);
            Calendar cab = Calendar.getInstance();
            cab.setTime(b);
            
            Integer beginYear = cab.get(Calendar.YEAR);
            Integer beginMon = cab.get(Calendar.MONTH);
            
            Date e = new SimpleDateFormat(dateFormat).parse(endValue);
            Calendar cae = Calendar.getInstance();
            cae.setTime(e);
            
            Integer endYear = cae.get(Calendar.YEAR);
            Integer endMon = cae.get(Calendar.MONTH);
            
            Integer spaceMon = ((endYear - beginYear) * 12 + endMon) - beginMon;
            
            if (spaceMon < 0){   // endDate smaller than beginDate
                return null;
            }
            
            Integer begin = this.calculate(beginValue);
            Integer end = this.calculate(endValue);
            
            Integer beginR = begin;
            Integer endR = end;
            
            while (beginR >= partitionDay) {
                beginR = beginR - partitionDay;
            }
            while (endR >= partitionDay) {
                endR = endR - partitionDay;
            }
            
            Integer partitionLen = 0;
            
            if (spaceMon > 0) {
                spaceMon -= 1;
                partitionLen = partitionDay - beginR + spaceMon * partitionDay + endR + 1;
            } else if (spaceMon == 0) { // at the same month
                partitionLen = endR - beginR + 1;
            }

            if (partitionLen >= totalPartitions) {
                re = new Integer[totalPartitions];
                for (int i = 0; i < totalPartitions; i++){
                    re[i] = i;
                }
            } else {
                re = new Integer[partitionLen];
                for (int i = 0; i < partitionLen; i++){
                    re[i] = (begin + i) % totalPartitions;
                }
            }

            return re;
        } catch (ParseException e) {
            throw new java.lang.IllegalArgumentException(e);
        }
    }
    
    public void setPartitionMon(Integer partitionMon) {
        if ((12 % partitionMon) != 0) {
            throw new java.lang.IllegalArgumentException("partitionMon must be 12's approximate number");
        }
        this.partitionMon = partitionMon;
    }

    public void setPartitionDay(Integer partitionDay) {
        if ((30 % partitionDay) != 0) {
            throw new java.lang.IllegalArgumentException("partitionDay must be 30's approximate number");
        }
        this.partitionDay = partitionDay;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    /**
     *
     * @author myubuntu
     * Jul 4, 2016 3:46:01 PM
     * PortalLogPartitioner.java
     * @param args
     * TODO : 
     *
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        /*System.out.println(31 / 5);
        System.out.println(29 / 5);
        System.out.println(26 / 5);
        System.out.println(23 / 5);*/

        HdtAccessPartitioner slp = new HdtAccessPartitioner();
        slp.setDateFormat("yyyy-MM-dd");
        slp.setPartitionMon(3);
        slp.setPartitionDay(5);
        slp.init();

        System.out.println(slp.calculate("2015-01-02"));
        System.out.println(slp.calculate("2015-01-05"));
        System.out.println(slp.calculate("2015-01-08"));
        System.out.println(slp.calculate("2015-01-13"));
        System.out.println(slp.calculate("2015-01-17"));
        System.out.println(slp.calculate("2015-01-21"));
        System.out.println(slp.calculate("2015-01-25"));
        System.out.println(slp.calculate("2015-01-31"));
        System.out.println(slp.calculate("2015-02-02"));
        System.out.println(slp.calculate("2015-02-12"));
        System.out.println(slp.calculate("2015-02-28"));
        
        System.out.println(slp.calculate("2015-03-04"));
        System.out.println(slp.calculate("2015-03-06"));
        System.out.println(slp.calculate("2015-03-09"));
        System.out.println(slp.calculate("2015-03-15"));
        System.out.println(slp.calculate("2015-03-19"));
        System.out.println(slp.calculate("2015-03-20"));
        System.out.println(slp.calculate("2015-03-25"));
        System.out.println(slp.calculate("2015-03-31"));
        System.out.println(slp.calculate("2015-04-02"));
        System.out.println(slp.calculate("2015-04-12"));
        System.out.println(slp.calculate("2015-04-28"));
        System.out.println(slp.calculate("2015-04-30"));

        System.out.println("=================1===================");
        Integer[] tmps = slp.calculateRange("2015-02-02", "2015-03-15");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("==================2==================");
        tmps = slp.calculateRange("2015-05-12", "2015-07-02");
        for (Integer i : tmps){
            System.out.println(i);
        }

        System.out.println("=================3===================");
        tmps = slp.calculateRange("2015-05-02", "2015-07-02");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("=================4===================");
        tmps = slp.calculateRange("2015-08-02", "2015-08-22");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("=================44===================");
        tmps = slp.calculateRange("2015-07-02", "2015-07-22");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("=================5===================");
        tmps = slp.calculateRange("2015-08-02", "2015-08-05");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("=================6===================");
        tmps = slp.calculateRange("2015-12-25", "2016-01-05");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("==================7==================");
        tmps = slp.calculateRange("2015-12-25", "2016-02-05");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("==================77==================");
        tmps = slp.calculateRange("2015-11-25", "2016-01-05");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("===================8=================");
        tmps = slp.calculateRange("2015-12-25", "2016-03-05");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("===================9=================");
        tmps = slp.calculateRange("2015-12-25", "2016-04-05");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("===================10=================");
        tmps = slp.calculateRange("2015-12-05", "2016-03-05");
        for (Integer i : tmps){
            System.out.println(i);
        }
        
        System.out.println("===================11=================");
        tmps = slp.calculateRange("2015-12-05", "2016-02-24");
        for (Integer i : tmps){
            System.out.println(i);
        }
    }

}
