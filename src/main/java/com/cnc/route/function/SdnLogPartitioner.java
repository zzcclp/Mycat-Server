package com.cnc.route.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;
import org.opencloudb.config.model.rule.RuleAlgorithm;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

/**
 * 按照月份分区
 * 
 * @author zhangzc
 * 
 */
public class SdnLogPartitioner extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    
	private static final Logger LOGGER = Logger
			.getLogger(SdnLogPartitioner.class);

	private Integer partitionMon;
	private String dateFormat;
	
	private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	@Override
    public void init() {
        if (dateFormat == null){
            dateFormat = DATETIME_FORMAT;
        }
    }

	@Override
	public Integer calculate(String columnValue) {
		try {
			Date targetTime = new SimpleDateFormat(dateFormat).parse(columnValue);
			Calendar ca = Calendar.getInstance();
            ca.setTime(targetTime);
            
            Integer mon = ca.get(Calendar.MONTH);
			
			int targetPartition = mon % partitionMon;
			return targetPartition;

		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);
		}
	}
	
	protected Integer[] calculateRangeHelper(String beginValue, String endValue){
	    try {
	        Integer[] re = new Integer[2];
	        
            Date b = new SimpleDateFormat(dateFormat).parse(beginValue);
            Calendar cab = Calendar.getInstance();
            cab.setTime(b);
            
            Date e = new SimpleDateFormat(dateFormat).parse(endValue);
            Calendar cae = Calendar.getInstance();
            cae.setTime(e);
            
            Integer yearB = cab.get(Calendar.YEAR);
            Integer monB = cab.get(Calendar.MONTH);
            
            Integer yearE = cae.get(Calendar.YEAR);
            Integer monE = cae.get(Calendar.MONTH);
            
            Integer reE = (yearE - yearB) * 12 + monE;
            if (reE < 0){
                return null;
            }
            
            re[0] = monB;
            re[1] = reE;
            
            return re;

        } catch (ParseException e) {
            throw new java.lang.IllegalArgumentException(e);
        }
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		//return AbstractPartitionAlgorithm.calculateSequenceRange(this, beginValue, endValue);
	    Integer begin = 0, end = 0;
        Integer[] beArr = this.calculateRangeHelper(beginValue, endValue); 

        if(beArr == null){
            return new Integer[0];
        }
        
        begin = beArr[0];
        end = beArr[1];
        
        int len = end - begin + 1;
            
        Integer[] re = null; //new Integer[len];
        
        if (len >= partitionMon){
            re = new Integer[partitionMon];
            for (int i = 0; i < partitionMon; i++){
                re[i] = i;
            }
        }
        else{
            re = new Integer[len];
            for (int i = 0; i < len; i++){
                int li = begin + i;
                if (li >= partitionMon){
                    li = li % partitionMon;
                }
                re[i] = li;
            }
        }
        return re;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

    public void setPartitionMon(Integer partitionMon) {
        this.partitionMon = partitionMon;
    }
    
    /*public static void main(String[] args){
        SdnLogPartitioner slp = new SdnLogPartitioner();
        slp.setDateFormat("yyyy-MM-dd");
        slp.setPartitionMon(12);
        
        System.out.println(slp.calculate("2015-01-02"));
        System.out.println(slp.calculate("2015-02-02"));
        System.out.println(slp.calculate("2015-03-02"));
        System.out.println(slp.calculate("2015-04-02"));
        System.out.println(slp.calculate("2015-05-02"));
        System.out.println(slp.calculate("2015-06-02"));
        System.out.println(slp.calculate("2015-07-02"));
        System.out.println(slp.calculate("2015-08-02"));
        System.out.println(slp.calculate("2015-09-02"));
        System.out.println(slp.calculate("2015-10-02"));
        System.out.println(slp.calculate("2015-11-02"));
        System.out.println(slp.calculate("2015-12-02"));
        
        System.out.println("====================================");
        Integer[] tmps = slp.calculateRange("2014-11-02", "2015-09-02");
        for (Integer i : tmps){
            System.out.println(i);
        }
    }*/

}
