package com.paiyue.scheduler;

//import java.io.FileNotFoundException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


//import com.paiyue.audienceStatisticTool.AudienceImeiStatsServ;
//import com.paiyue.audienceStatisticTool.AudienceIosStatsServ;
//import com.paiyue.audienceStatisticTool.AudienceStatisticService;
import com.paiyue.audienceStatisticTool.AudienceStatsToolServ;
import com.paiyue.update.UpdateFromHadoop;

public class Scheduler {
		
	// 定时查询数据库，获取audienceList值
		private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);  
	    ScheduledFuture<?> taskHandle;  
		public void taskSchedule() {  
			
			AudienceStatsToolServ asts=new AudienceStatsToolServ();
	        // 定期到数据库中查询人群标签
	        final Runnable task1 = new Runnable() {  
	            public void run() {  
	            	try{
	            		asts.setAudienceCategoryList();
	            	}catch(NullPointerException e){
	            		e.printStackTrace();
	            	}
	            }  
	        };  
	        //定期读取人群库到内存中
//	        final Runnable task2 = new Runnable() {  
//	            public void run() {  
//	            	try {
//						asts.loadAudienceTagArr(new AudienceStatisticService());
//						asts.loadAudienceTagArr(new AudienceIosStatsServ());
//						asts.loadAudienceTagArr(new AudienceImeiStatsServ());
//					} catch (NullPointerException | IndexOutOfBoundsException
//							| FileNotFoundException e) {
//						e.printStackTrace();
//					}
//	            }  
//	        };  
	        //每周日更新一次人群库
	        final Runnable task3 = new Runnable() {  
	            public void run() {  
	            	Calendar c=Calendar.getInstance();
	            	int day=c.get(Calendar.DAY_OF_WEEK);
	            	//当前日期为周日时，执行更新
	            	if(day==1) {
	            		System.out.println("update app start...");
	            		try{
	            			new UpdateFromHadoop().update();
	            		}catch(Exception e){
	            			System.out.println(new Date()+":------更新错误-----");
	            			e.printStackTrace();
	            		}
	            		System.out.println("update app end");
	            	}
	            }  
	        };  
	        taskHandle = scheduler.scheduleAtFixedRate(task1, 0, 3600, TimeUnit.SECONDS);  
//	        taskHandle = scheduler.scheduleAtFixedRate(task2, 0, 3600, TimeUnit.SECONDS);  
	        taskHandle = scheduler.scheduleAtFixedRate(task3, 0, 86400, TimeUnit.SECONDS);  
	    }  
}
