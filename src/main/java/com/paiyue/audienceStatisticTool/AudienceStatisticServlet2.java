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
 * Servlet implementation class AudienceStatisticServlet2
 */
public class AudienceStatisticServlet2 extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public AudienceStatisticServlet2() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 * 根据传入条件，返回符合条件的人数和总基数
	 * request包括 tagStr和driver两个参数
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) {
		try{
			
			String tagStr = request.getParameter("tagStr");
			String driver = request.getParameter("driver");
			AudienceStatisticController ad = new AudienceStatisticController();
			AudienceNum match = ad.getLibMatchNum(tagStr,driver);
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
