package com.paiyue.audienceStatisticTool;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paiyue.bean.AudienceNum;

/**
 * Servlet implementation class AudiStatsOneDay
 */
public class AudiStatsOneDay extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public AudiStatsOneDay() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try{
			
			String tagStr = request.getParameter("tagStr");
			String driver = request.getParameter("driver");
			AudienceStatisticController ad = new AudienceStatisticController();
			AudienceNum match = ad.getMatchNum(tagStr,driver);
			String json=getJsonStrByClass(match);
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
	public static String getJsonStrByClass(AudienceNum match){
		ObjectMapper mapper = new ObjectMapper();
		String res="";
		try {
			res = mapper.writeValueAsString(match);
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
