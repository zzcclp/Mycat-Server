/**
 *
 * @author zhangzc
 * Jul 4, 2016 3:46:00 PM
 * PortalLogPartitioner.java
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
 * PortalLogPartitioner.java
 * TODO : 
 *
 */
public class PortalLogPartitioner extends AbstractPartitionAlgorithm implements
RuleAlgorithm {

    private static final Logger LOGGER = Logger
            .getLogger(PortalLogPartitioner.class);

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
            
            Date e = new SimpleDateFormat(dateFormat).parse(beginValue);
            Calendar cae = Calendar.getInstance();
            cae.setTime(e);
            
            Integer endYear = cae.get(Calendar.YEAR);
            Integer endMon = cae.get(Calendar.MONTH);
            
            Integer diffMon = (endYear - beginYear) * 12 + endMon;
            if (diffMon < 0){
                return null;
            }
            
            int monLen = diffMon - beginMon + 1;
            
            if ((diffMon - beginMon) >= partitionMon) { // return all partitions
                
            } else {
                
            }

            Integer begin = this.calculate(beginValue);
            Integer end = this.calculate(endValue);

            if (end >= begin) {

            } else {

            }

            return re;
        } catch (ParseException e) {
            throw new java.lang.IllegalArgumentException(e);
        }
    }
    
    public void setPartitionMon(Integer partitionMon) {
        this.partitionMon = partitionMon;
    }

    public void setPartitionDay(Integer partitionDay) {
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

        PortalLogPartitioner plp = new PortalLogPartitioner();
        plp.setDateFormat("yyyy-MM-dd");
        plp.setPartitionMon(2);
        plp.setPartitionDay(6);
        plp.init();

        System.out.println(plp.calculate("2015-01-02"));
        System.out.println(plp.calculate("2015-01-08"));
        System.out.println(plp.calculate("2015-01-12"));
        System.out.println(plp.calculate("2015-01-25"));
        System.out.println(plp.calculate("2015-01-31"));
        System.out.println(plp.calculate("2015-02-01"));
        System.out.println(plp.calculate("2015-02-12"));
        System.out.println(plp.calculate("2015-02-20"));
        System.out.println(plp.calculate("2015-02-28"));
        System.out.println(plp.calculate("2015-03-02"));
        System.out.println(plp.calculate("2015-04-12"));
        System.out.println(plp.calculate("2015-05-22"));
        
        /*
        System.out.println("====================================");
        Integer[] tmps = plp.calculateRange("2014-11-02", "2015-09-02");
        for (Integer i : tmps){
            System.out.println(i);
        }*/
    }

}
