package com.paiyue.audienceStatisticTool;

//import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paiyue.scheduler.Scheduler;
//import com.paiyue.update.Update;
import com.paiyue.update.UpdateFromHadoop;

/**
 * Servlet implementation class AudienceStatisticServlet
 */

public class AudienceStatisticServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public AudienceStatisticServlet() {
        super();
    }
    
    /**
     * 启动时候加载标签库和人群库
     */
    static{
    		
//    		AudienceStatsToolServ asts=new AudienceStatsToolServ();
//    		asts.setAudienceCategoryList();//加载人群标签表
//	    	if(new File(asts.getParentPath()+"/data").exists()&&new File(asts.getParentPath()+"/iosdata").exists()
//	    			&&new File(asts.getParentPath()+"/imeidata").exists()){
//	    		
//	    		System.out.println("加载cookieid库");
//		    	asts.loadAudienceTagArr(new AudienceStatisticService());
//		    	System.out.println("加载imei库");
//		    	asts.loadAudienceTagArr(new AudienceImeiStatsServ());
//		    	System.out.println("加载ios库");
//		    	asts.loadAudienceTagArr(new AudienceIosStatsServ());
//		    	
//	    	}else{
//	    		if(!new Update().update()){
//	    			throw new Exception("初始更新失败！");
//	    		}
//	    	}
//		
    	long start1=System.currentTimeMillis();
    	Long beforeMemory1 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory(); 
    	UpdateFromHadoop ufh=new UpdateFromHadoop();
		ufh.update();
		Long beforeMemory2 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		long end1=System.currentTimeMillis();
		System.out.println("人群数据 内存使用:" + (double)(beforeMemory2 - beforeMemory1) / 1024.000 / 1024.000 + "MB"); 
		System.out.println("用时:"+(end1-start1)+"ms");
	
		//开启定时器
		Scheduler s=new Scheduler();
		s.taskSchedule();	
    		
	}
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) {
		try{
			
			String tagStr = request.getParameter("tagStr");
			String driver = request.getParameter("driver");
			AudienceStatisticController ad = new AudienceStatisticController();
			
			Map<String, Object>  map = ad.calculateAudiNum(tagStr,driver);
			String json = getJsonStrByMap(map);
			
			response.setContentType("text/html; charset=utf-8");
			response.getWriter().append(json);
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	/**
	 * 返回json数组的字符串
	 * @param map
	 * @return
	 */
	public static String getJsonStrByMap(Map<String,Object> map){
		ObjectMapper mapper = new ObjectMapper();
		String res="";
		try {
			res = mapper.writeValueAsString(map);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
		return res;
		
	}
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.doGet(request, response);
	}

}
