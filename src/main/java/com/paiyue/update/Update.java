//package com.paiyue.update;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.math.BigDecimal;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileUtil;
//import org.apache.hadoop.fs.Path;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.paiyue.audience.full.AudienceDatabase;
//import com.paiyue.audienceStatisticTool.AudienceImeiStatsServ;
//import com.paiyue.audienceStatisticTool.AudienceIosStatsServ;
//import com.paiyue.audienceStatisticTool.AudienceStatisticController;
//import com.paiyue.audienceStatisticTool.AudienceStatisticService;
//import com.paiyue.audienceStatisticTool.AudienceStatsToolServ;
//import com.paiyue.bean.AudienceCategory;
//import com.paiyue.bean.AudienceInterface;
//import com.paiyue.bean.AudienceNum;
//
//
//public class Update {
//	
//	private static Configuration configuration = new Configuration();
//	
//	static {
//		System.setProperty("HADOOP_USER_NAME", "na.ma");
//		configuration.set("dfs.block.size", String.valueOf(256 * 1024 * 1024));
//		configuration.set("dfs.replication", String.valueOf(2));
//		configuration.set("dfs.replication.min", String.valueOf(2));
//		configuration.set("fs.defaultFS", "hdfs://bigdatacluster");
////		configuration.set("fs.default.name", configuration.get("fs.defaultFS"));
//		configuration.set("dfs.nameservices","bigdatacluster");
//		configuration.set("dfs.ha.namenodes.bigdatacluster", "nn2,nn1");
//		configuration.set("dfs.namenode.rpc-address.bigdatacluster.nn1","10.1.0.120:8020");
//		configuration.set("dfs.namenode.rpc-address.bigdatacluster.nn2", "10.1.0.121:8020");
//		configuration.set("dfs.client.failover.proxy.provider.bigdatacluster","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//		configuration.set("fs.hdfs.impl", 
//		        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//		    );
//		configuration.set("fs.file.impl",
//		        org.apache.hadoop.fs.LocalFileSystem.class.getName()
//		    );
//	}
//	private static String parentPath="/home/paiyue/mana/manapy/audience";
//	public boolean update(){
//		String parent=parentPath;//本地根路径
//		//cookie相关文件路径
//		String hdfsCookiepath="/user/na.ma/cookieSample3";//hadoop上的cookie id路径
//		//ios相关文件路径
//		String hdfsIospath="/user/na.ma/iosSample3";//hadoop上的ios路径
//		//imei相关文件路径
//		String hdfsImeipath="/user/na.ma/imeiSample3";//hadoop上的imei路径
//		FileSystem hdfs = null;
//		try {
//			setAudiList();
//			hdfs = FileSystem.get(configuration);
//			updateData(hdfs,"PC",parent,hdfsCookiepath,"/data","/old","/rate","/cookieid");
//			updateData(hdfs,"IOS",parent,hdfsIospath,"/iosdata","/oldios","/iosrate","/iosid");
//			updateData(hdfs,"Imei",parent,hdfsImeipath,"/imeidata","/oldimei","/imeirate","/imeiid");
//			return true;
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally{
//			if(hdfs!=null){
//				try {
//					hdfs.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		return false;
//	}
//	private void setAudiList() throws NullPointerException{
//		AudienceStatsToolServ asts=new AudienceStatsToolServ();
//		List<AudienceCategory> list=asts.getAllCategoryList();
//		if(list==null||list.size()==0){
//			throw new NullPointerException("加载人群标签表失败！");
//		}
//		asts.setAudienceList(list);
//	}
//	//每周日执行一次cookie更新
//	private void updateData(FileSystem hdfs,String flag,String parent,String hdfspath,String dataRPath,String oldRPath
//			,String rPath,String idPath) throws Exception{
//		
//		String rateRootPath=parent+"/audimesg";//存储数据库中人群标签数据和比例文件的根路径
//		String idRootP=parent+"/hadoop";//存储从hadoop上读取的cookie id文件的根路径
//		String upRootPath=parent+"/updateData";//更新文件的根路径
//		String upAudienceCookieFlag=upRootPath+"/audienceCookieFlag.txt";//更新文件 人群库的路径
//		String upCountTagSum=upRootPath+"/countTagSum.txt";//更新文件 标签数量文件路径
//		int sampleSum=15835000;//读取的cookie id数量
////		int sampleSum=150;//读取的cookie id数量
//		String dataRootPath=parent+dataRPath;//处理后的数据文件根路径，存储最终所需的数据
//		String oldRootPath=parent+oldRPath;//上一次数据存储的根路径
//		
//		String ratePath=upRootPath+rPath;//读取cookie数量/cookie总量=比例文件
//		String oldRatePath=oldRootPath+rPath;//上一次的比例文件
//		String cookieIdPath=idRootP+idPath;//从hadoop读取的指定数量cookieid的文件路径
//		
//		
//			if(isOrNotSuccess(hdfspath,hdfs)&&createDir(idRootP)&&createDir(upRootPath)&&createDir(dataRootPath)&&createDir(rateRootPath)){
//				if(new File(cookieIdPath).exists()){
//					new File(cookieIdPath).delete();
//				}
//				System.out.println("hadoop下有更新数据，开始读取数据...");
//				//从hdfs读取cookieSampleSum条数据到本地，并返回总条数
//				int audiSum=readFromHdfs(hdfs,hdfspath,cookieIdPath,sampleSum);
////				int audiSum=146;
//				System.out.println("总共有"+audiSum+"条数据，读取"+sampleSum+"数据完毕，开始在audience数据库匹配标签...");
//				//将cookie和AudienceDatebase匹配得到cookie的标签
//				String ex="";
//				if("IOS".equals(flag)){
//					ex="ios_idfa:";
//				}else if("Imei".equals(flag)){
//					ex="imei_md5:";
//				}
//				int realSum=getCookieTag(ex,cookieIdPath,upAudienceCookieFlag);
////				int realSum=100;
//				System.out.println("数据匹配完毕，开始生成标签统计文件...");
//				//分析得到cookie标签库的统计文件，包括标签和所占的个数，总人数等
//				getCounttag(upAudienceCookieFlag,upCountTagSum);
//				float rate=new BigDecimal(String.valueOf(audiSum)).divide(new BigDecimal(String.valueOf(realSum)), 2, BigDecimal.ROUND_DOWN).floatValue();
//				wrRate2File(rate,audiSum,ratePath);
//				System.out.println("文件生成完毕，开始读取到内存，并替换原数据...");
//				//生成文件后替换原有数据	
//				AudienceInterface obj=null;
//				if("IOS".equals(flag)){
//					obj=new AudienceIosStatsServ();
//				}else if("Imei".equals(flag)){
//					obj=new AudienceImeiStatsServ();
//				}else if("PC".equals(flag)){
//					obj=new AudienceStatisticService();
//				}
//				replaceAudiLib(obj,upAudienceCookieFlag,upCountTagSum,upRootPath,dataRootPath
//						,oldRootPath,ratePath,oldRatePath,audiSum,rate);
//				System.out.println("替换完毕！");
//			}else{
//				throw new Exception("hadoop下没有执行成功的文件！");
//			}
//	}
//	//创建文件夹
//	private boolean createDir(String path){
//		File dir=new File(path);
//		if(!dir.exists()&&!dir.isDirectory()){
//			if(!dir.mkdir()){
//				return false;
//			}
//		}
//		return true;
//	}
//	//hadoop上是否成功执行文件
//	private boolean isOrNotSuccess(String hdfspath,FileSystem hdfs) {
//		try{
//			FileStatus[] fs = hdfs.listStatus(new Path(hdfspath));
//			Path[] listPath = FileUtil.stat2Paths(fs);
//			for(Path p : listPath){
//				  if(p.getName().equals("_SUCCESS")){
//					  return true;
//				  }
//			}
//		}catch( IOException e){
//			e.printStackTrace();
//		}
//		return false;    
//	}
//	//从hdfs读取cookieSampleSum条数据
//	private int readFromHdfs(FileSystem hdfs,String hdfspath,String cookieIdPath,int cookieSampleSum) throws IOException{
//		  FileStatus[] fs;
//		  BufferedWriter out = null;
//		  BufferedReader br=null;
//		  int count=0;//统计读取的cookie条数
//		  int audiSum=0;//一天unbid数据的总cookie数
//		  try{
//			  fs = hdfs.listStatus(new Path(hdfspath));
//			  Path[] listPath = FileUtil.stat2Paths(fs);
//			  out=new BufferedWriter(new FileWriter(cookieIdPath));
//			  //循环文件列表，读取文件内容
//			  for(Path p : listPath){
//				  br=new BufferedReader(new InputStreamReader(hdfs.open(p)));
//				  String line="";
//				  while((line=br.readLine())!=null){
//					  if(count != -1){
//						  out.write(line);  
//						  out.newLine();
//						  count++;
//						  if(count==cookieSampleSum){//取cookieSampleSum条数据
//							  count=-1;
//						  }
//					  }
//					  audiSum++;
//				  }
//				  br.close();
//			  }
//		  }catch(IOException e){
//			  e.printStackTrace();
//			  throw new IOException("更新-从hadoop读取数据失败！");
//		  }finally{
//			  if(br!=null){
//				  try {
//					br.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			  }
//			  if(out!=null){
//				  try {
//					out.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			  }
//		  }
//		  return audiSum;
//	}
//	
//	//获取日志中的cookie，并到audiencedatabase中获取对应的标签，写入文件中
//	private int getCookieTag(String ex,String cookieIdPath,String upAudienceCookieFlag) throws IOException{
//		if(new File(upAudienceCookieFlag).exists()){
//			new File(upAudienceCookieFlag).delete();
//		}
//		Map.Entry<Long, Map<Short, Double>> result;
//		BufferedReader br=null;
//		BufferedWriter bw=null;
//		int count=0;
//		try{
//			br=new BufferedReader(new FileReader(cookieIdPath));//抽取指定数量
//			bw=new BufferedWriter(new FileWriter(upAudienceCookieFlag));
//			String idKey="";
//			AudienceDatabase ad = new AudienceDatabase();
//			while((idKey=br.readLine())!=null){
//				result = ad.getAudience(ex+idKey);
//				if(result!=null){
//					bw.write(idKey+": "+transMapToString(result.getValue()));
//					bw.newLine();
//					count++;
//					if(count%1000000==0){
//						Date now = new Date();
//				    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//				    	String time = dateFormat.format( now );
//						System.out.println(time+":已匹配"+count+"条cookie");
//					}
//				}
//			}
//			System.out.println("共有"+count+"条cookie匹配");
//		}catch(IOException e){
//			e.printStackTrace();
//			throw new IOException("更新-匹配标签失败！");
//		}finally{
//			if(br!=null){
//				try {
//					br.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//			if(bw!=null){
//				try {
//					bw.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		return count;
//	}	
//	
//	//将map转换为string
//	private String transMapToString(Map<Short, Double> map) {
//		StringBuffer sb = new StringBuffer(200);
//		for (Map.Entry<Short, Double> entry : map.entrySet()) {
//			sb.append(entry.getKey()).append('=')
//					.append(new BigDecimal(entry.getValue().toString()).setScale(2, BigDecimal.ROUND_HALF_UP).toString()).append(';');
//		}
//		return sb.toString();
//	}		
//	
//	//得到统计标签数量的文件
//	private void getCounttag(String upAudienceCookieFlag,String upCountTagSum) throws IOException{
//		if(new File(upCountTagSum).exists()){
//			new File(upCountTagSum).delete();
//		}
//		BufferedReader br=null;
//		BufferedWriter bw=null;
//		try{
//			br=new BufferedReader(new FileReader(upAudienceCookieFlag));
//			bw=new BufferedWriter(new FileWriter(upCountTagSum));
//			Map<String,Integer> map=new HashMap<String,Integer>();
//			String cookietag="";int cookiesum=0;
//			while((cookietag=br.readLine())!=null){ //读文件
//				String[] str=cookietag.split(": ");
//				if(str.length>1){ //带标签的cookie
//					String[] str2=str[1].split(";");//拆分出一个cookie的标签
//					int length=str2.length;
//					for(int i=0;i<length;i++){
//						int count=0;
//						String id=str2[i].split("=")[0];
//						if(map.containsKey(id)){
//							count=map.get(id);
//						}
//						count++;
//						map.put(id, count);
//					}
//					cookiesum++;
//				}
//			}
//			bw.write(String.valueOf(cookiesum));bw.newLine();
//			for(Map.Entry<String, Integer> entry:map.entrySet()){
//				bw.write(entry.getKey()+"\t"+entry.getValue());
//				bw.newLine();
//			}
//		}catch(IOException e){
//			e.printStackTrace();
//			throw new IOException("更新-产生标签统计文件失败！");
//		}finally{
//			if(br!=null){
//				try {
//					br.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//			if(bw!=null){
//				try {
//					bw.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//	}		
//				
//	//将文件拷贝到系统读取的指定目录下，并将文件读入到内存中，替换当前使用的人群库
//	@SuppressWarnings("unchecked")
//	private void replaceAudiLib(AudienceInterface ass,String upAudienceCookieFlag,String upCountTagSum,String updateRootPath
//			,String dataRootPath,String oldRootPath,String ratePath,String oldRatePath,int audiSum,float rate) throws Exception{
//		
//		AudienceStatsToolServ asts=new AudienceStatsToolServ();
//		Map<String,Object> topMap=asts.createTagMap(upCountTagSum);
//		Map<Short,Integer> upMap =(Map<Short,Integer>)topMap.get("map");
//		Map<Short,Integer> upMapSum = (Map<Short,Integer>)topMap.get("mapSum");
//		List<Short> upList = (List<Short>)topMap.get("list");
//		long[][] upBitMap=asts.getBitMapArr2(upAudienceCookieFlag,upMap);
//		//当全部值都读入，且符合条件时，修改文件并替换原有内存
//		if( upBitMap!=null && upBitMap.length>0 && upMap != null && upMap.size()>1
//				&& upMapSum != null && upMapSum.size() > 0 && upList != null && upList.size() > 0 && rate > 0 
//				&& audiSum>0 &&renameDir(updateRootPath,dataRootPath,oldRootPath)){
//			
//			//赋值给系统中的静态变量
//			ass.setAudienceTagMap(upMap);//audienceTagMap key:数字 value:标签id
//			ass.setAudienceTagList(upList);//audienceTagList 与上同，下标：数字 下标存储的数据：标签id
//			ass.setAudienceTagSum(upMapSum);//audienceTagSum key:标签id vlaue:标签id对应的数量
//			ass.setAudienceTag(upBitMap);//audienceTag cookieid对应的数字及数字对应的标签
//			ass.setRate(rate);
//			ass.setAudiSum(audiSum);
//			ass.setLibRate(new BigDecimal(String.valueOf(ass.getLibNum()/(float)audiSum)).setScale(2, BigDecimal.ROUND_DOWN).floatValue());				
//			
//			
//		}else{
//			throw new Exception("更新数据中有不符合条件的数据!");
//		}
//	}
//
//	//重命名文件夹，upRootPath命名为rootPath，oldRootPath删掉，rootPath命名为oldRootPath
//	private boolean renameDir(String upRootPath,String rootPath,String oldRootPath){
//		File upRootFile=new File(upRootPath);
//		File rootFile=new File(rootPath);
//		File oldRootFile=new File(oldRootPath);
//		String[] files=upRootFile.list();
//		String[] fileNow=rootFile.list();
//		if(files!=null&&files.length==3&&new File(upRootFile,files[0]).length()>0&&new File(upRootFile,files[1]).length()>0&&new File(upRootFile,files[2]).length()>0){
//			if(fileNow!=null&&fileNow.length==3&&new File(rootFile,fileNow[0]).length()>0&&new File(rootFile,fileNow[1]).length()>0&&new File(rootFile,fileNow[2]).length()>0){
//				if(oldRootFile.exists()&&oldRootFile.isDirectory()){
//					if(deleteDir(oldRootFile)&&rootFile.renameTo(oldRootFile)&&upRootFile.renameTo(rootFile)){
//						return true;
//					}
//				}else{
//					if(rootFile.renameTo(oldRootFile)&&upRootFile.renameTo(rootFile)){
//						return true;
//					}
//				}
//			}else{
//				if(deleteDir(rootFile)&&upRootFile.renameTo(rootFile)){
//					return true;
//				}
//			}
//		}else{
//			deleteDir(upRootFile);
//		}
//		return false;
//	}
//	private boolean deleteDir(File dir) {
//        if (dir.isDirectory()) {
//            String[] children = dir.list();
//            for (int i=0; i<children.length; i++) {
//                boolean success = deleteDir(new File(dir, children[i]));
//                if (!success) {
//                    return false;
//                }
//            }
//        }
//        // The directory is now empty so now it can be smoked
//        return dir.delete();
//    }
//	//重命名ratePath文件为newRatePath，并将rate写入ratePath中
////	private boolean renameRateFile(float rate,int audiSum,String ratePath,String newRatePath){
////		if(renameFile(ratePath,newRatePath)&&wrRate2File(rate,audiSum,ratePath)){
////			return true;
////		}
////		return false;
////	}
//	//重命名文件名
//	public static boolean renameFile(String path,String newNamePath){
//		File pathFile=new File(path);
//		if(pathFile.exists()){
//			if(new File(newNamePath).exists()){
//				new File(newNamePath).delete();
//			}
//			if(pathFile.renameTo(new File(newNamePath))){
//				return true;
//			}
//		}else{
//			return true;
//		}
//		return false;
//	}		
//	//将比例写入到文件中
//	private void wrRate2File(float rate,int audiSum,String ratePath) throws IOException{
//		BufferedWriter bw=null; 
//		try{
////			 float rate=new BigDecimal(String.valueOf(audiSum)).divide(new BigDecimal(String.valueOf(count)), 2, BigDecimal.ROUND_HALF_UP).floatValue();
//			 bw=new BufferedWriter(new FileWriter(ratePath,true));
//			 bw.write(String.valueOf(rate));
//			 bw.newLine();
//			 bw.write(String.valueOf(audiSum));
//			 bw.close();
//		 }catch(IOException e){
//			 e.printStackTrace();
//			 throw new IOException("更新-写入比例文件失败！");
//		 }finally{
//			 if(bw!=null){
//				 try {
//					bw.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			 }
//		 }
//	}		
//	public static void main(String args[]){
//		Update ufh=new Update();
//		ufh.update();
//		//http://10.1.0.40:8500/audienceTool?tagStr=30003&driver=PC
//		
//		String tagStr = "30003";
//		String driver = "PC";
//		AudienceStatisticController ad = new AudienceStatisticController();
//		
//		Map<String, Object>  map = ad.calculateAudiNum(tagStr,driver);
//		String json = getJsonStrByMap(map);
//		System.out.println(json);
//		driver = "IOS";
//		map = ad.calculateAudiNum(tagStr,driver);
//		json = getJsonStrByMap(map);
//		System.out.println(json);
//		driver = "Imei";
//		map = ad.calculateAudiNum(tagStr,driver);
//		json = getJsonStrByMap(map);
//		System.out.println(json);
//		driver = "App";
//		map = ad.calculateAudiNum(tagStr,driver);
//		json = getJsonStrByMap(map);
//		System.out.println(json);
//		
//		String tagStr2 = "30003";
//		String driver2 = "PC";
//		
//		AudienceNum match = ad.getLibMatchNum(tagStr2,driver2);
//		String json2=getJsonStrByClass(match);
//		System.out.println(json2);
//		driver2 = "IOS";
//		match = ad.getLibMatchNum(tagStr2,driver2);
//		json2=getJsonStrByClass(match);
//		System.out.println(json2);
//		driver2 = "Imei";
//		match = ad.getLibMatchNum(tagStr2,driver2);
//		json2=getJsonStrByClass(match);
//		System.out.println(json2);
//		driver2 = "App";
//		match = ad.getLibMatchNum(tagStr2,driver2);
//		json2=getJsonStrByClass(match);
//		System.out.println(json2);
//		
//		String tagStr3 = "30003";
//		String driver3 = "PC";
//		AudienceNum match2 = ad.getMatchNum(tagStr3,driver3);
//		String json3=getJsonStrByClass(match2);
//		System.out.println(json3);
//		 driver3 = "IOS";
//		 match2 = ad.getMatchNum(tagStr3,driver3);
//		 json3=getJsonStrByClass(match2);
//		System.out.println(json3);
//		 driver3 = "Imei";
//		 match2 = ad.getMatchNum(tagStr3,driver3);
//		 json3=getJsonStrByClass(match2);
//		System.out.println(json3);
//		 driver3 = "App";
//		 match2 = ad.getMatchNum(tagStr3,driver3);
//		 json3=getJsonStrByClass(match2);
//		System.out.println(json3);
//		
//		AudienceImeiStatsServ aa=new AudienceImeiStatsServ();
//		System.out.println("imei:librate--"+aa.getLibRate()+"rate--"+aa.getRate());
//		
//		AudienceIosStatsServ a2=new AudienceIosStatsServ();
//		System.out.println("ios:librate--"+a2.getLibRate()+"rate--"+a2.getRate());
//		AudienceStatisticService as=new AudienceStatisticService();
//		System.out.println("pc:librate--"+as.getLibRate()+"rate--"+as.getRate());
//	}
//	public static String getJsonStrByMap(Map<String,Object> map){
//		ObjectMapper mapper = new ObjectMapper();
//		String res="";
//		try {
//			res = mapper.writeValueAsString(map);
//		} catch (JsonProcessingException e) {
//			throw new RuntimeException(e);
//		}
//		return res;
//		
//	}
//	public static String getJsonStrByClass(AudienceNum match){
//		ObjectMapper mapper = new ObjectMapper();
//		String res="";
//		try {
//			res = mapper.writeValueAsString(match);
//		} catch (JsonProcessingException e) {
//			throw new RuntimeException(e);
//		}
//		return res;
//		
//	}
//}
