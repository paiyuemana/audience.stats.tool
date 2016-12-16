package com.paiyue.update;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paiyue.audienceStatisticTool.AudienceStatisticController;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.paiyue.audienceStatisticTool.AudienceStatisticController;
import com.paiyue.audienceStatisticTool.AudienceStatsToolServ;
import com.paiyue.bean.AudienceCategory;
import com.paiyue.bean.AudienceImeiStatsServ;
import com.paiyue.bean.AudienceInterface;
import com.paiyue.bean.AudienceIosStatsServ;
import com.paiyue.bean.AudienceMobileWebServ;
import com.paiyue.bean.AudienceNum;
import com.paiyue.bean.AudienceStatisticService;

public class UpdateFromHadoop {
	
	private static Configuration configuration = new Configuration();
	static {
		System.setProperty("HADOOP_USER_NAME", "na.ma");
		configuration.set("dfs.block.size", String.valueOf(256 * 1024 * 1024));
		configuration.set("dfs.replication", String.valueOf(2));
		configuration.set("dfs.replication.min", String.valueOf(2));
		configuration.set("fs.defaultFS", "hdfs://bigdatacluster");
		configuration.set("dfs.nameservices","bigdatacluster");
		configuration.set("dfs.ha.namenodes.bigdatacluster", "nn2,nn1");
		configuration.set("dfs.namenode.rpc-address.bigdatacluster.nn1","10.1.0.120:8020");
		configuration.set("dfs.namenode.rpc-address.bigdatacluster.nn2", "10.1.0.121:8020");
		configuration.set("dfs.client.failover.proxy.provider.bigdatacluster","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//		configuration.set("fs.hdfs.impl", 
//		        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//		    );
//		configuration.set("fs.file.impl",
//		        org.apache.hadoop.fs.LocalFileSystem.class.getName()
//		    );
	}
//	private static String parentPath=System.getProperty("audi.stats.dir");
	private String cookie_libnum = "791037826";//pc site 和 移动web的cookie总数，即派悦库中人群数量
	private String siteAppSum_path="/user/na.ma/siteAppSmapleCount";
	private String oldSiteApp_path="/user/na.ma/oldSiteAppSmapleCount";
	/**
	 * 更新WEB,APP数据
	 * @return
	 */
	public void update() throws Exception{

		//site和app数据的matched和day人群数量
		
		FileSystem hdfs = null;

		try {
			setAudiList();//更新人群标签
			hdfs = FileSystem.get(configuration);
			//连接hadoop
//			Map<String,String> site_app_sample = getFromHadoop(hdfs,siteAppSum_path,oldSiteApp_path);//读取site和app端样本人群数和一天的人群数
//			updateData(hdfs,"PC",site_app_sample.get("matched-cookieid"),site_app_sample.get("day-cookieid"));
//			updateData(hdfs,"IOS",site_app_sample.get("matched-iosid"),site_app_sample.get("day-iosid"));
//			updateData(hdfs,"Imei",site_app_sample.get("matched-imeiid"),site_app_sample.get("day-imeiid"));
			
			updateData(hdfs,"PC");
			updateData(hdfs,"IOS");
			updateData(hdfs,"Imei");
			updateData(hdfs,"Moweb");//移动web
			
		} catch (Exception e) {
			throw new Exception("数据更新失败，异常信息："+e.toString());
		}finally{
			if(hdfs!=null){
				try {
					hdfs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 更新人群标签
	 * @throws NullPointerException
	 */
	private void setAudiList(){
		List<AudienceCategory> list=AudienceStatsToolServ.getAllCategoryList();
		if(list==null||list.size()==0){
			throw new RuntimeException("加载人群标签表失败！");
		}
		AudienceStatsToolServ.setAudienceList(list);
	}
	private Map<String,String> getFromHadoop(FileSystem hdfs,String path) {
			Map<String,String> map=new HashMap<String,String>();
			FileStatus[] fs;
			BufferedReader br=null;
			try{
				  fs = hdfs.listStatus(new Path(path));
				  Path[] listPath = FileUtil.stat2Paths(fs);
				  //循环文件列表，读取文件内容
				  for(Path p : listPath){
					  br=new BufferedReader(new InputStreamReader(hdfs.open(p)));
					  String line="";
					  while((line=br.readLine())!=null){
						  String[] str=line.split(":");
						  map.put(str[0], str[1]);
					  }
					  br.close();
				  }
				  return map;
			  }catch(IOException e){
				  throw new RuntimeException(e);
			  }finally{
				  if(br!=null){
					  try {
						br.close();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				  }
			  }
		}
		
		private void updateData(FileSystem hdfs,String flag) {
			System.out.println("hadoop下有更新数据，开始读取数据...");
			AudienceInterface obj=null;
			String idMatched_path="";
			String tagSum_path="";
			String siteApp_path="";
			if("IOS".equals(flag)){
				obj=new AudienceIosStatsServ();
			}else if("Imei".equals(flag)){
				obj=new AudienceImeiStatsServ();
			}else if("PC".equals(flag)){
				obj=new AudienceStatisticService();
			}else if("Moweb".equals(flag)){
				obj=new AudienceMobileWebServ();
			}
			if(obj==null){
				throw new RuntimeException(flag+"参数类型不符合要求！参数类型：IOS , Imei , PC, Moweb");
			}
			if(!isOrNotSuccess(obj.getIdMatched_path(),hdfs)||!isOrNotSuccess(obj.getIdTagSum_path(),hdfs)
					||!isOrNotSuccess(siteAppSum_path,hdfs)){
				if(!isOrNotSuccess(obj.getOldIdMatchedPath(),hdfs)||!isOrNotSuccess(obj.getOldTagSumPath(),hdfs)
						||!isOrNotSuccess(oldSiteApp_path,hdfs)){
					throw new RuntimeException(idMatched_path+"或"+tagSum_path+"或"+siteAppSum_path+"等新旧文件均不存在！");
				}
				idMatched_path=obj.getOldIdMatchedPath();
				tagSum_path=obj.getOldTagSumPath();		
				siteApp_path=oldSiteApp_path;
				System.out.println(new Date()+":"+flag+"--------------------使用old文件，新文件不存在！------------------");
			}else{
				idMatched_path=obj.getIdMatched_path();
				tagSum_path=obj.getIdTagSum_path();
				siteApp_path=siteAppSum_path;
			}
			Map<String,String> site_app_sample = getFromHadoop(hdfs,siteApp_path);//读取site和app端样本人群数和一天的人群数
			replaceAudiLib(obj,hdfs,idMatched_path,tagSum_path,site_app_sample);
			System.out.println("更新完毕！");
		}
		//hadoop上是否成功执行文件
		private boolean isOrNotSuccess(String hdfspath,FileSystem hdfs) {
			try{
				FileStatus[] fs = hdfs.listStatus(new Path(hdfspath));
				Path[] listPath = FileUtil.stat2Paths(fs);
				for(Path p : listPath){
					  if(p.getName().equals("_SUCCESS")){
						  return true;
					  }
				}
			}catch( IOException e){
				e.printStackTrace();
			}
			return false;    
		}
		//将hadoop上的文件读取到内存，并替换当前使用的人群库
		@SuppressWarnings("unchecked")
		private void replaceAudiLib(AudienceInterface ass,FileSystem hdfs,String idMatched_path,String tagSum_path,Map<String,String> site_app_sample) {
			
			String idMatched_count=site_app_sample.get(ass.getIdMatched_key());
			String idDay_count=site_app_sample.get(ass.getIdDay_key());
			AudienceStatsToolServ asts=new AudienceStatsToolServ();
			Map<String,Object> topMap=asts.createTagMap(hdfs,tagSum_path,idMatched_count);
			Map<Short,Integer> upMap =(Map<Short,Integer>)topMap.get("map");
			Map<Short,Integer> upMapSum = (Map<Short,Integer>)topMap.get("mapSum");
			List<Short> upList = (List<Short>)topMap.get("list");
			long[][] upBitMap=asts.getBitMapArr2(hdfs,idMatched_path,upMap);
			
			//当全部值都读入，且符合条件时，修改文件并替换原有内存
			if( upBitMap!=null && upBitMap.length>0 && upMap != null && upMap.size()>1 && 
					upMapSum != null && upMapSum.size() > 0 && upList != null && upList.size() > 0 && 
					Integer.parseInt(idDay_count)>0 && Integer.parseInt(idMatched_count)>0 ){
				
				//赋值给系统中的静态变量
				ass.setAudienceTagMap(upMap);//audienceTagMap key:数字 value:标签id
				ass.setAudienceTagList(upList);//audienceTagList 与上同，下标：数字 下标存储的数据：标签id
				ass.setAudienceTagSum(upMapSum);//audienceTagSum key:标签id vlaue:标签id对应的数量
				ass.setAudienceTag(upBitMap);//audienceTag cookieid对应的数字及数字对应的标签
				ass.setRate(new BigDecimal(idDay_count).divide(new BigDecimal(idMatched_count), 2, BigDecimal.ROUND_DOWN).floatValue());
				ass.setAudiSum(Integer.parseInt(idDay_count));
				if((ass instanceof AudienceMobileWebServ)||(ass instanceof AudienceStatisticService)){
					String cookieday_count=site_app_sample.get("day-cookieid");//pc site一天cookie的数量
					String mowebday_count=site_app_sample.get("day-mowebcookie");//移动web一天cookie的数量
					//比例=当前传入类型(day-cookieid/day-mowebcookie)的当天cookie数量/当天cookie总量
					float assInLibRate=new BigDecimal(idDay_count).divide(new BigDecimal(cookieday_count).add(new BigDecimal(mowebday_count)), 2, BigDecimal.ROUND_HALF_UP).floatValue();
					//当前传入类型在派悦库中总量=派悦库中总cookie量×当前传入类型pc/移动web所占当天库量的占比
					ass.setLibNum(new BigDecimal(this.cookie_libnum).multiply(new BigDecimal(assInLibRate)).setScale(0, BigDecimal.ROUND_HALF_UP).longValue());
				}
				ass.setLibRate(new BigDecimal(String.valueOf(ass.getLibNum()/Float.parseFloat(idDay_count))).setScale(2, BigDecimal.ROUND_DOWN).floatValue());				
			}else{
				throw new RuntimeException("当前最新数据不满足更新条件，更新失败!");
			}
		}
		public static void main(String args[]){
		UpdateFromHadoop ufh=new UpdateFromHadoop();
		try {
			ufh.update();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//http://10.1.0.40:8500/audienceTool?tagStr=30003&driver=PC
		
		String tagStr = "30003";
		String driver = "PC";
		AudienceStatisticController ad = new AudienceStatisticController();
		
		Map<String, Object>  map = ad.calculateAudiNum(tagStr,driver);
		String json = getJsonStrByMap(map);
		System.out.println(json);
		driver = "IOS";
		map = ad.calculateAudiNum(tagStr,driver);
		json = getJsonStrByMap(map);
		System.out.println(json);
		driver = "Imei";
		map = ad.calculateAudiNum(tagStr,driver);
		json = getJsonStrByMap(map);
		System.out.println(json);
		driver = "App";
		map = ad.calculateAudiNum(tagStr,driver);
		json = getJsonStrByMap(map);
		System.out.println(json);
		
		String tagStr2 = "30003";
		String driver2 = "PC";
		
		AudienceNum match = ad.getLibMatchNum(tagStr2,driver2);
		String json2=getJsonStrByClass(match);
		System.out.println(json2);
		driver2 = "IOS";
		match = ad.getLibMatchNum(tagStr2,driver2);
		json2=getJsonStrByClass(match);
		System.out.println(json2);
		driver2 = "Imei";
		match = ad.getLibMatchNum(tagStr2,driver2);
		json2=getJsonStrByClass(match);
		System.out.println(json2);
		driver2 = "App";
		match = ad.getLibMatchNum(tagStr2,driver2);
		json2=getJsonStrByClass(match);
		System.out.println(json2);
		
		String tagStr3 = "30003";
		String driver3 = "PC";
		AudienceNum match2 = ad.getMatchNum(tagStr3,driver3);
		String json3=getJsonStrByClass(match2);
		System.out.println(json3);
		 driver3 = "IOS";
		 match2 = ad.getMatchNum(tagStr3,driver3);
		 json3=getJsonStrByClass(match2);
		System.out.println(json3);
		 driver3 = "Imei";
		 match2 = ad.getMatchNum(tagStr3,driver3);
		 json3=getJsonStrByClass(match2);
		System.out.println(json3);
		 driver3 = "App";
		 match2 = ad.getMatchNum(tagStr3,driver3);
		 json3=getJsonStrByClass(match2);
		System.out.println(json3);
		
		AudienceImeiStatsServ aa=new AudienceImeiStatsServ();
		System.out.println("imei:librate--"+aa.getLibRate()+"rate--"+aa.getRate());
		
		AudienceIosStatsServ a2=new AudienceIosStatsServ();
		System.out.println("ios:librate--"+a2.getLibRate()+"rate--"+a2.getRate());
		AudienceStatisticService as=new AudienceStatisticService();
		System.out.println("pc:librate--"+as.getLibRate()+"rate--"+as.getRate());
	}
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
}
