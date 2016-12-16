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

public class AudienceStatisticServlet extends HttpServlet{
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public AudienceStatisticServlet() throws ServletException{
        super();
    }
    /**
     * 初始化调用
     */
    public void init() throws ServletException{
    	long start1=System.currentTimeMillis();
    	UpdateFromHadoop ufh=new UpdateFromHadoop();
		try {
			ufh.update();
			//开启定时器
    		Scheduler s=new Scheduler();
    		s.taskSchedule();
		} catch (Exception e) {
			throw new ServletException("程序启动失败，数据加载异常！"+e.toString());
		}
		long end1=System.currentTimeMillis();
		System.out.println("用时:"+(end1-start1)+"ms");
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
