package com.paiyue.audienceStatisticTool;

import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.paiyue.bean.AudienceCategory;
//import com.paiyue.bean.AudienceInterface;
import com.paiyue.bean.AudienceStatisticModel;

public class AudienceStatsToolServ {
	//根目录
	private static String parentPath=System.getProperty("audi.stats.dir");
	
	private static List<AudienceCategory> audienceList ;//储存策略编辑页面需要的人群定向所使用的标签

	public void setAudienceCategoryList() throws NullPointerException{//加载人群标签表
		if(getAudienceList()==null||getAudienceList().size() == 0){
			List<AudienceCategory> list=getAllCategoryList();
			if(list==null||list.size()==0){
				throw new NullPointerException("加载人群标签表失败！");
			}
			setAudienceList(list);
		}
	}
	
	public static List<AudienceCategory> getAllCategoryList() {//读取人群标签表
		List<AudienceCategory> list=new ArrayList<AudienceCategory>();
		BufferedReader br=null;
		String line="";
		try {
//			br=new BufferedReader(new FileReader(this.getParentPath()+"/audimesg/audimesg.txt"));
//			br=new BufferedReader(new FileReader("/home/paiyue/mana/manapy/audience/audimesg/audimesg.txt"));
			br=new BufferedReader(new FileReader("/home/dataapi/tomcat-8500/audi_stats_dir/audimesg/audimesg.txt"));
			while((line=br.readLine())!=null){
				String[] str=line.split(",");
				AudienceCategory ac=new AudienceCategory();
				ac.setId(Short.parseShort(str[0]));
				ac.setRawData(str[2]);
				list.add(ac);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return list;
	}
	
//	@SuppressWarnings("unchecked")
//	public void loadAudienceTagArr(AudienceInterface ai) throws NullPointerException,IndexOutOfBoundsException,FileNotFoundException{//加载人群标签数据
//		String parentPath=getParentPath();
//		//人群标签统计的文件
//		String countTagSum=parentPath+ai.getTagSumPath();
//		//人群库的文件
//		String audienceCookieFlag=parentPath+ai.getAudiIdFlagPath();
//		//比例和基数的文件
//		String ratePath=parentPath+ai.getRatePath();
//		
//		if(fileNotLegal(countTagSum,audienceCookieFlag,ratePath)){
//			countTagSum=parentPath+ai.getOldTagSumPath();
//			audienceCookieFlag=parentPath+ai.getOldAudiIdFlagPath();
//			ratePath=parentPath+ai.getOldRatePath();
//			if(fileNotLegal(countTagSum,audienceCookieFlag,ratePath)){
//				throw new FileNotFoundException("加载人群数据时，未找到文件！");
//			}
//		}
//		if(ai.getAudienceTagMap() == null || ai.getAudienceTagMap().size() < 2 || ai.getAudienceTagSum() == null 
//				|| ai.getAudienceTagSum().size() == 0 || ai.getAudienceTagList() == null 
//				|| ai.getAudienceTagList().size() ==0 || ai.getRate() == 0 || ai.getAudiSum() == 0
//				|| ai.getAudienceTag() == null || ai.getAudienceTag().length == 0){
//			
//			Map<String,Object> topMap=createTagMap(countTagSum);
//			if(topMap!=null&&topMap.get("map")!=null&&topMap.get("mapSum")!=null&&topMap.get("list")!=null){
//				ai.setAudienceTagMap((Map<Short,Integer>)topMap.get("map"));
//				ai.setAudienceTagSum((Map<Short,Integer>)topMap.get("mapSum"));
//				ai.setAudienceTagList((List<Short>)topMap.get("list"));
//			}else{
//				throw new NullPointerException("读取标签数量统计文件失败！");
//			}
//			String[] rStr=rRateFrFile(ratePath).split(",");
//			if(rStr.length>1){
//				float rate=Float.valueOf(rStr[0]);
//				int audiSum=Integer.valueOf(rStr[1]);
//				if(audiSum>0){
//					float libRate=new BigDecimal(String.valueOf(ai.getLibNum()/(float)audiSum)).setScale(2, BigDecimal.ROUND_DOWN).floatValue();
//					if(rate>0&&libRate>0){//计算总库与一天的量的比值
//						ai.setRate(rate);
//						ai.setAudiSum(audiSum);
//						ai.setLibRate(libRate);
//					}else{
//						throw new ArithmeticException("加载总库比例时，一天的量过多或者为零！");
//					}
//				}else{
//					throw new ArithmeticException("加载总库比例时，一天的人群数量小于0！");
//				}
//			}else{
//				throw new IndexOutOfBoundsException("加载rate到内存时缺少值！");
//			}
//			long[][] bitMap=getBitMapArr2(audienceCookieFlag,ai.getAudienceTagMap());
//			if(bitMap != null && !"".equals(bitMap)){
//				ai.setAudienceTag(getBitMapArr2(audienceCookieFlag,ai.getAudienceTagMap()));
//			}else{
//				throw new NullPointerException("读取抽样库文件，生成bitmap失败！");
//			}
//		}	
//	}
	
//	private boolean fileNotLegal(String countTagSum,String audienceCookieFlag,String ratePath){
//		File ctsFile=new File(countTagSum);
//		File acfFile=new File(audienceCookieFlag);
//		File rpFile=new File(ratePath);
//		if(!ctsFile.exists()||ctsFile.length()==0||!acfFile.exists()||acfFile.length()==0||!rpFile.exists()||rpFile.length()==0){
//			return true;
//		}
//		return false;
//	}
	
	/**
	 * 读取人群标签到map中
	 */
	public Map<String,Object> createTagMap(String path) {
		Map<String,Object> topMap=new HashMap<String,Object>();
		Map<Short,Integer> map=new HashMap<Short,Integer>();
		Map<Short,Integer> mapSum=new HashMap<Short,Integer>();
		List<Short> list=new ArrayList<Short>();
		BufferedReader br=null;
		try {
			br = new BufferedReader(new FileReader(path));
			String line="";
			int count=-1;
			while((line=br.readLine())!=null){
				String[] tagStr=line.split("\t");
				if(tagStr.length==1){
					map.put(Short.parseShort("0"), Integer.parseInt(line));
				}else{
					short id=Short.parseShort(tagStr[0]);
					int tagSum=Integer.parseInt(tagStr[1]);
					map.put(id, count);
					list.add(id);
					//读取标签数量
					mapSum.put(id, tagSum);
				}
				count++;
			}	
			topMap.put("list", list);
			topMap.put("mapSum", mapSum);
			topMap.put("map", map);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return topMap;
	}
	/**
	 * 读取hadoop上标签id-数量文件到map中
	 */
	public Map<String,Object> createTagMap(FileSystem hdfs,String path,String idMatched_count) {
		Map<String,Object> topMap=new HashMap<String,Object>();//存储一下三个变量
		Map<Short,Integer> map=new HashMap<Short,Integer>();//audienceTagMap key:数字 value:标签id
		Map<Short,Integer> mapSum=new HashMap<Short,Integer>();//标签id与标签个数
		List<Short> list=new ArrayList<Short>();//下标对应数字，存储对应标签id
		BufferedReader br=null;
		FileStatus[] fs;
		try {
			  fs = hdfs.listStatus(new Path(path));
			  Path[] listPath = FileUtil.stat2Paths(fs);
			  map.put(Short.parseShort("0"), Integer.parseInt(idMatched_count));
			  int count=0;//标签对应的数字，递增
			  
			  //循环文件列表，读取文件内容
			  for(Path p : listPath){
				  br=new BufferedReader(new InputStreamReader(hdfs.open(p)));
				  String line="";
				  while((line=br.readLine())!=null){
						String[] tagStr=line.split("\t");
						short id=Short.parseShort(tagStr[0]);
						int tagSum=Integer.parseInt(tagStr[1]);
						map.put(id, count);
						list.add(id);
						//读取标签数量
						mapSum.put(id, tagSum);
						count++;
					}	
			  }
			topMap.put("list", list);
			topMap.put("mapSum", mapSum);
			topMap.put("map", map);
			return topMap;
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 
	 * 读取比例和基数到内存中
	 */
	public String rRateFrFile(String ratePath){
		BufferedReader br=null;
		String line="";
		String rStr="";
		int count=0;
		try{
			br=new BufferedReader(new FileReader(ratePath));
			while((line=br.readLine())!=null){
				if(count==0){
					rStr+=line+",";
				}else if(count==1){
					rStr+=line;
				}
				count++;
			}
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return rStr;
	}
	
	/**
	 * 读取hadoop上id-匹配标签文件到内存中
	 * */
	public long[][] getBitMapArr2(FileSystem hdfs,String audienceTagPath,Map<Short,Integer> map) {
		int bitmaplen=getLen(map.get(Short.parseShort("0")));//bitmap 64位的个数
		int sublength=map.size()-1;//bitmap个数
		long[][] bitMap=null;
		String line="";
		int count=0;
		BufferedReader br=null;
		FileStatus[] fs;
		try {
			fs = hdfs.listStatus(new Path(audienceTagPath));
			  Path[] listPath = FileUtil.stat2Paths(fs);
			  bitMap=new long[sublength][bitmaplen];//记录整个人群库的标签
			  //循环文件列表，读取文件内容
			  for(Path p : listPath){
				  br=new BufferedReader(new InputStreamReader(hdfs.open(p)));
				  while((line=br.readLine())!=null){
						String[] s=line.split(": ");
						if(s.length>1){
							String[] tagId=s[1].split(";");
							int tagIdLen=tagId.length;
							for(int i=0;i<tagIdLen;i++){
								setBit(bitMap[map.get(Short.parseShort(tagId[i].split("=")[0]))], count);
							}
							count++;
						}
					}
			  }
			  return bitMap;
		} catch (Exception e) {
			System.out.println(audienceTagPath+"文件读取失败！");
			throw new RuntimeException(e);
		} finally{
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 读取人群标签文件到内存中
	 * 
	 * */
	public long[][] getBitMapArr2(String audienceTagPath,Map<Short,Integer> map) {
		int bitmaplen=getLen(map.get(Short.parseShort("0")));//bitmap 64位的个数
		int sublength=map.size()-1;//bitmap个数
		long[][] bitMap=null;
		String line="";
		int count=0;
		BufferedReader br=null;
		try {
			bitMap=new long[sublength][bitmaplen];//记录整个人群库的标签
			br = new BufferedReader(new FileReader(audienceTagPath));
			//读取人群标签文件到内存中
			while((line=br.readLine())!=null){
				String[] s=line.split(": ");
				if(s.length>1){
					String[] tagId=s[1].split(";");
					int tagIdLen=tagId.length;
					for(int i=0;i<tagIdLen;i++){
						setBit(bitMap[map.get(Short.parseShort(tagId[i].split("=")[0]))], count);
					}
					count++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return bitMap;
	}
	/**
	 * 符合条件的人群标签归类，并统计各类数量
	 * param：map key:rowData;map value:标签数量
	 */
	@SuppressWarnings("unchecked")
	public Map<String,Object> getMatchTagMap(int baseSum,Map<String,int[]> map){
		Map<String, Object> populationPropsMap = new HashMap<String, Object>();
		int sexSum=0;int ageSum=0;int incomeSum=0;int educationSum=0;
		int professionSum=0;
		int industrySum=0;
		int lifePeriodSum=0;
		int attentionSum=0;
		int buySum=0;
		int count=0;
		for (Map.Entry<String, int[]> entry : map.entrySet()) {
			String key = "";
			String entryKey = entry.getKey();
			int[] entryValue = entry.getValue();
			if (entry.getKey().indexOf("人口属性") != -1) {
				String[] tagGroup = entryKey.split("/");
				if(tagGroup.length >= 3){
					List<AudienceStatisticModel> propsMap = new ArrayList<AudienceStatisticModel>();
					if (entry.getKey().indexOf("性别") != -1) {
						key = "sex";sexSum+=entryValue[0];
					} else if (entry.getKey().indexOf("年龄") != -1) {
						key = "age";ageSum+=entryValue[0];
					} else if (entry.getKey().indexOf("月收入") != -1) {
						key = "income";incomeSum+=entryValue[0];
					} else if (entry.getKey().indexOf("受教育程度") != -1) {
						key = "education";educationSum+=entryValue[0];
					} else if (entry.getKey().indexOf("身份职业") != -1) {
						key = "profession";professionSum+=entryValue[0];
					} else if (entry.getKey().indexOf("各行业专业人员") != -1) {
						key = "industry";industrySum+=entryValue[0];
					} else if (entry.getKey().indexOf("关键人生阶段") != -1) {
						key = "lifePeriod";lifePeriodSum+=entryValue[0];
					}
					if (populationPropsMap.containsKey(key)) {
						propsMap = (ArrayList<AudienceStatisticModel>)populationPropsMap.get(key);
					}
					AudienceStatisticModel asm=new AudienceStatisticModel();
					asm.setAudiName(entryKey.substring(entryKey.lastIndexOf("/") + 1, entryKey.length()));
					asm.setTagSum(entryValue[0]);
					asm.setTagTotalSum(entryValue[1]);
					//占比另算
					propsMap.add(asm);
					populationPropsMap.put(key, propsMap);
				}
			}else {
				String[] tagGroup = entryKey.split("/");
				if(tagGroup.length == 2){
					String name=tagGroup[1];
					if(entry.getKey().indexOf("兴趣关注") != -1){
						key="attention";attentionSum+=entryValue[0];
					}else if(entry.getKey().indexOf("消费倾向") != -1){
						key="buy";buySum+=entryValue[0];
					}
					Map<String,AudienceStatisticModel> newTagMap=new HashMap<>();
					if(populationPropsMap.containsKey(key)){
						newTagMap=(Map<String,AudienceStatisticModel>)populationPropsMap.get(key);
					}
					AudienceStatisticModel asm=new AudienceStatisticModel();
					asm.setAudiName(name);
					asm.setTagSum(entryValue[0]);
					asm.setTagTotalSum(entryValue[1]);
					newTagMap.put(name, asm);
					populationPropsMap.put(key, newTagMap);
				}
				if("符合条件的人数".equals(tagGroup[0])){
					count=entryValue[0];
				}
			}
		}	
 		populationPropsMap.put("sexAudiSum",sexSum);
		populationPropsMap.put("ageAudiSum",ageSum);
		populationPropsMap.put("incomeAudiSum",incomeSum);
		populationPropsMap.put("educationAudiSum",educationSum);
		populationPropsMap.put("professionAudiSum",professionSum);
		populationPropsMap.put("industryAudiSum",industrySum);
		populationPropsMap.put("lifePeriodAudiSum",lifePeriodSum);
		populationPropsMap.put("attentionAudiSum",attentionSum);
		populationPropsMap.put("buyAudiSum",buySum);
		populationPropsMap.put("tagMatchSum",count);
		populationPropsMap.put("baseSum",baseSum);
		
		return populationPropsMap;
	}
	
	/**
	 * 对存储的人群标签数据进行统计，统计符合传入条件tagArr的数据
	 * 参数：tagStr 表示用户选择的条件
	 * 返回值：map key指标签的rawData，map value是一个数组，长度为2，其中int[0]表示符合条件人中该标签数量，int[1]表示整个标签库中该标签数量
	 */
	public Map<String, int[]> countAudiences2(Map<Short,Integer> tagSumMap
			,List<Short> tagList,long[][] bitMap,Map<Short,Integer> map,String tagStr){
//		Map<Short,Integer> map=getAudienceTagMap();//标签id、标签id对应的数字
//		long[][] bitMap=getAudienceTag();//人群库的cookie标签数据
		long[] bitMapAnd=countAudiNum(bitMap,map,tagStr);//根据输入条件统计人数，按位存储，1表示有，0表示没有
		
		//在符合条件的人群中，找到具有性别标签和年龄标签的人
		short[] sexId=new short[]{30003,30004};
		long[] haveSex=getHaveTag(map,bitMapAnd,bitMap,sexId);
		short[] ageId=new short[]{30007,30008,30009,30010,30006};
		long[] haveAge=getHaveTag(map,bitMapAnd,bitMap,ageId);
		int haveSexSum=this.getNum(haveSex);//在符合条件的人群中，具有性别标签的人数
		int haveAgeSum=this.getNum(haveAge);//在符合条件的人群中，具有年龄标签的人数
		int count=this.getNum(bitMapAnd);//计算符合条件的总人数
		
//		List<Short> tagList=getAudienceTagList();//short存储了标签id，对应的list下标为该标签对应的数字，为了便于通过下标直接获得标签id
		Map<Short,Integer> mapSum=getMatchTagNum(tagList,bitMap,bitMapAnd);//存储标签id及在符合条件人群中该标签数量
		
		Map<String, int[]> audienceTagMap = new HashMap<String, int[]>();//存储人群标签raw data以及对应数量
		List<AudienceCategory> audiList=getAudienceList();//从数据库中查询到的人群标签列表
//		Map<Short,Integer> tagSumMap=getAudienceTagSum();//每个标签在人群库中的数量
		for (AudienceCategory audienceCategory : audiList) {
			Short id=audienceCategory.getId();
			int[] sumArr=new int[]{(mapSum.containsKey(id)?mapSum.get(id):0),(mapSum.containsKey(id)?tagSumMap.get(id):0)};//sumArr[0] 标签id符合条件的数量，sumArr[1]该标签id在人群库中占的数量
			audienceTagMap.put(audienceCategory.getRawData(), sumArr);
		}
		int[] sumArr1=new int[]{count-haveSexSum,0};
		audienceTagMap.put("人口属性/性别/未知", sumArr1);
		int[] sumArr2=new int[]{count-haveAgeSum,0};
		audienceTagMap.put("人口属性/年龄/未知", sumArr2);
		int[] tagCount=new int[]{count,0};
		audienceTagMap.put("符合条件的人数", tagCount);//符合条件的总人数
		return audienceTagMap;
	}
	
	/**
	 *根据tagStr计算符合条件的人数
	 *返回数组总位数表示总人数，使用每一位置1或置0标志该位置是否满足条件
	 *最终统计1的个数即可
	 *
	 * */
	public long[] countAudiNum(long[][] bitMap,Map<Short,Integer> map,String tagStr){
//		Map<Short,Integer> map=getAudienceTagMap();//标签id、标签id对应的数字
//		long[][] bitMap=getAudienceTag();//人群库的cookie标签数据
		int len=getLen(map.get(Short.parseShort("0")));
		String[] tagOr=tagStr.split("&");
		int tagOrlen=tagOr.length;
		long[] bitMapAnd=new long[len];//最终得到的人数
		for(int i=0;i<tagOrlen;i++){
			String[] tagAnd=tagOr[i].split("\\|");
			int tagAndlen=tagAnd.length;
			long[] bitMapOr=new long[len];//进行与操作的数组
			for(int j=0;j<tagAndlen;j++){
				short tagId=Short.parseShort(tagAnd[j]);
				if(map.containsKey(tagId)){
					int idtoint=map.get(tagId);//得到标签id对应的数字 
					if(j==0){
						bitMapOr=bitMap[idtoint];
					}else{
						bitMapOr=orTwoBit(bitMap[idtoint],bitMapOr);
					}
				}
			}
			if(i==0){
				bitMapAnd=bitMapOr;
			}else{
				bitMapAnd=andTwoBit(bitMapAnd,bitMapOr);
			}
		}
		return bitMapAnd;
	}
	
	/**
	 *获取符合条件的人群中各个标签的个数
	 * */
	private Map<Short,Integer> getMatchTagNum(List<Short> list,long[][] bitMap,long[] bitMapAnd){
		Map<Short,Integer> mapSum=new HashMap<Short,Integer>();
		int len=bitMap.length;
		for(int i=0;i<len;i++){//循环人群库中的每个标签
			
			int count=getNum(andTwoBit(bitMap[i],bitMapAnd));//对符合条件的人群bitmap和每个标签的人群bitmap相与
																										//得到的是具有该标签的人群数
			mapSum.put(list.get(i), count);//将具有该标签的人数放到map中
		}
		return mapSum;
	}
	
	
	/**
	 *将id数组中对应的bitmap进行或操作，再与bitMapAnd进行与操作
	 * */
	private long[] getHaveTag(Map<Short,Integer> map,long[] bitMapAnd,long[][] bitMap,short[] id){
		int len=id.length;
		long[] haveTag=new long[bitMapAnd.length];
		for(int i=0;i<len;i++){
			if(map.containsKey(id[i])){
				int idtoint=map.get(id[i]);//得到标签id对应的数字
				if(i==0){
					haveTag=bitMap[idtoint];
				}else{
					haveTag=orTwoBit(bitMap[idtoint],haveTag);
				}
			}
		}
		haveTag=andTwoBit(bitMapAnd,haveTag);
		return haveTag;
	}
	/**
	 *统计bit中1的个数
	 * */
	public int getNum(long[] bit){
		int len=bit.length;
		int count=0;
		for(int i=0;i<len;i++){
			long temp=bit[i];
			while(temp!=0){
				temp=temp&(temp-1);
				count++;
			}
		}
		return count;
	}
	
	/**
	 *两个bit进行|操作
	 * */
	public long[] orTwoBit(long[] oneBit,long[] twoBit){
		int bitlen=twoBit.length;
		long[] result=new long[bitlen];
		for(int j=0;j<bitlen;j++){		//循环传入的条件标签
			result[j]=oneBit[j]|twoBit[j];
		}
		return result;
	}
	
	/**
	 *两个bit进行操作
	 *
	 * */
	public long[] andTwoBit(long[] oneBit,long[] twoBit){
		int bitlen=twoBit.length;
		long[] result=new long[bitlen];
		for(int j=0;j<bitlen;j++){		//循环传入的条件标签
			result[j]=oneBit[j]&twoBit[j];
		}
		return result;
	}
	
	/**
	 * 求占比
	 * @param totalSum
	 * @param map
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> initRate(int totalSum,Map<String, Object> map) {
//		int totalSum=getAudienceTagMap().get(Short.parseShort("0"));
		int count=(int)map.get("tagMatchSum");//符合条件的人数
		for(Map.Entry<String, Object> entry:map.entrySet()){
			String key=entry.getKey();
			int tagSum=count;
			if (map.get(key) instanceof ArrayList<?>) {
				if("sex".equals(key)||"age".equals(key)){
					tagSum=(int)map.get(key+"AudiSum");
				}
				List<AudienceStatisticModel> list=(List<AudienceStatisticModel>)map.get(key);
				for (AudienceStatisticModel audienceStatisticModel : list) {
					// 获取访客占比
					if (tagSum != 0) {
						audienceStatisticModel.setTagRate((float) 1.0 * audienceStatisticModel.getTagSum() / tagSum);
						if(audienceStatisticModel.getTagTotalSum()!=0){
							audienceStatisticModel.setTgiRate((((float) 1.0 * audienceStatisticModel.getTagSum() / count) / ((float) 1.0 * audienceStatisticModel.getTagTotalSum() / totalSum))*100);
						}
					}
				}
			}else if(map.get(key) instanceof HashMap<?,?>){
				Map<String,AudienceStatisticModel> tagMap=(Map<String,AudienceStatisticModel>)map.get(key);
				for (Map.Entry<String, AudienceStatisticModel> entry2:tagMap.entrySet()) {
					AudienceStatisticModel audienceStatisticModel=entry2.getValue();
					// 获取访客占比
					if (tagSum != 0) {
						audienceStatisticModel.setTagRate((float) 1.0 * audienceStatisticModel.getTagSum() / tagSum);
						if(audienceStatisticModel.getTagTotalSum()!=0){
							audienceStatisticModel.setTgiRate((audienceStatisticModel.getTagRate() / ((float) 1.0 * audienceStatisticModel.getTagTotalSum() / totalSum))*100);
						}
					}
				}
			}
		}
		return map;
	}
	@SuppressWarnings("unchecked")
	public Map<String, Object> multiRate(float rate,Map<String, Object> map) {
		for(Map.Entry<String, Object> entry:map.entrySet()){
			String key=entry.getKey();
			if (map.get(key) instanceof ArrayList<?>) {
				List<AudienceStatisticModel> list=(List<AudienceStatisticModel>)map.get(key);
				for (AudienceStatisticModel audienceStatisticModel : list) {
					audienceStatisticModel.setTagSum(this.rMultyInt(rate,audienceStatisticModel.getTagSum()));
					audienceStatisticModel.setTagTotalSum(this.rMultyInt(rate,audienceStatisticModel.getTagTotalSum()));
				}
			}else if(map.get(key) instanceof HashMap<?,?>){
				Map<String,AudienceStatisticModel> tagMap=(Map<String,AudienceStatisticModel>)map.get(key);
				for (Map.Entry<String, AudienceStatisticModel> entry2:tagMap.entrySet()) {
					AudienceStatisticModel audienceStatisticModel=entry2.getValue();
					audienceStatisticModel.setTagSum(this.rMultyInt(rate,audienceStatisticModel.getTagSum()));
					audienceStatisticModel.setTagTotalSum(this.rMultyInt(rate,audienceStatisticModel.getTagTotalSum()));
				}
			}else if(map.get(key) instanceof Integer){
				if(!key.equals("baseSum")){
					int sum=this.rMultyInt(rate,(int) map.get(key));
					map.put(key, sum);
				}
			}
		}
		return map;
	}
	/**
	 * 设置柱状图数据
	 * @param map
	 * @param type
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public StringBuilder InitBar(Map<String, Object> map,String type) {
		
		StringBuilder resultBar = new StringBuilder();
		List<AudienceStatisticModel> list=new ArrayList<>();
		if (map.get(type) instanceof ArrayList<?>) {
			list = (List<AudienceStatisticModel>) map.get(type);
		}else if(map.get(type) instanceof HashMap<?,?>){
			list=new ArrayList<AudienceStatisticModel>(((Map<String,AudienceStatisticModel>) map.get(type)).values());
		}
		this.getSortedListBySum(list);
		for (AudienceStatisticModel AudienceStatisticModel : list) {
			resultBar.append("{name:'");
			resultBar.append(AudienceStatisticModel.getAudiName());
			resultBar.append("',tagSum:'");
			resultBar.append(AudienceStatisticModel.getTagSum());
			resultBar.append("',tagRate:'");
			resultBar.append(AudienceStatisticModel.getTagRate());
			resultBar.append("',tgiRate:'");
			resultBar.append(AudienceStatisticModel.getTgiRate());
			resultBar.append("'},");
		}
		resultBar = this.setBar(resultBar);	
		return resultBar;
	}

	/**
	 * 设置柱状图最小显示
	 * @param stringBuilder
	 * @return
	 */
	public StringBuilder setBar(StringBuilder stringBuilder) {
		int SIZE = 5;// 默认柱状图最少个数
		String[] data = stringBuilder.toString().split("},");
		int size = data.length;
		if (size >= SIZE) {
			return stringBuilder;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = (SIZE - size) / 2; i >= 1; i--) {
			sb.append("{name:'NA");
			sb.append("',tagSum:'0");
			sb.append("',tagRate:'0");
			sb.append("',tgiRate:'0");
			sb.append("'},");
		}

		sb.append(stringBuilder);
		for (int i = 1; i <= (SIZE - size) / 2 + (SIZE - size) % 2; i++) {
			sb.append("{name:'NA");
			sb.append("',tagSum:'0");
			sb.append("',tagRate:'0");
			sb.append("',tgiRate:'0");
			sb.append("'},");
		}
		return sb;
	}

	/**
	 * 给list按照AudienceStatisticModel中sum排序
	 * @param list
	 */
	public void getSortedListBySum(List<AudienceStatisticModel> list) {
		Collections.sort(list, new Comparator<AudienceStatisticModel>() {
            public int compare(AudienceStatisticModel o1, AudienceStatisticModel o2) {
            	int preValue = o1.getTagSum();
				int behValue = o2.getTagSum();
				return preValue > behValue ? -1 : (preValue < behValue) ? 1 : 0;
            }
        });
	}
	
	/**
	 *统计符合条件的总人数
	 *@param rate
	 *@param bitMap
	 *@param map
	 *@param tagStr
	 * */
	public int getTotalNum(float rate,long[][] bitMap,Map<Short,Integer> map,String tagStr){
		return this.rMultyInt(rate,this.getNum(this.countAudiNum(bitMap,map,tagStr)));
	}
	
	/**
	 *整数和rate相乘，返回结果
	 *@param rate
	 *@param i
	 *@return
	 * */
	public int rMultyInt(float rate,int i){
		return (int)(new BigDecimal(String.valueOf(i)).multiply(new BigDecimal(String.valueOf(rate))).doubleValue());
	}
	
	/**
	 *整数和rate相乘，返回结果
	 *
	 * */
	public long rMultyInt(float rate,long i){
		return (long)(new BigDecimal(String.valueOf(i)).multiply(new BigDecimal(String.valueOf(rate))).doubleValue());
	}
	
	
	public int quotient(int number){
		  return ((number) >> 6);
	}
	public int remainder(int number){
		return (number % 64);
	}
	public int getLen(int number){
		return ((number>>6)+1);
	}
	public void setBit(long[] bitData,int number){
		 bitData[quotient(number)] |= (0x01L << remainder(number));
	}
	public String getParentPath(){
		return parentPath;
	}
	public static List<AudienceCategory> getAudienceList() {
		return audienceList;
	}
	public static void setAudienceList(List<AudienceCategory> audilist) {
		audienceList = audilist;
	}
}
