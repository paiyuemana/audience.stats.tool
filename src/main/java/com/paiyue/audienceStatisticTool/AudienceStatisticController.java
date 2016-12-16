package com.paiyue.audienceStatisticTool;

import java.util.HashMap;
import java.util.Map;

import com.paiyue.bean.*;

public class AudienceStatisticController {
	/**
	 * 人群工具
	 * 
	 * 马娜
	 * 2015-12-21
	 */
	private AudienceStatsToolServ asts=new AudienceStatsToolServ();
	
	/**
	 * 查询符合tagStr条件的人群数量
	 */
	public Map<String,Object> calculateAudiNum(String tagStr,String type) {
		try{
			if("PC".equalsIgnoreCase(type)){
				return this.getMatchTagData(new AudienceStatisticService(),tagStr);
			}else if("IOS".equalsIgnoreCase(type)){
				return this.getMatchTagData(new AudienceIosStatsServ(),tagStr);
			}else if("Imei".equalsIgnoreCase(type)){
				return this.getMatchTagData(new AudienceImeiStatsServ(),tagStr);
			}else if("App".equalsIgnoreCase(type)){
				return this.getAppMatchData(new AudienceIosStatsServ(), new AudienceImeiStatsServ(), tagStr);
			}else if("Moweb".equalsIgnoreCase(type)){//移动web
				return this.getMatchTagData(new AudienceMobileWebServ(),tagStr);
			}else if("Mobile".equalsIgnoreCase(type)){//移动端
				return this.getMobileMatchData(new AudienceIosStatsServ(), new AudienceImeiStatsServ(), new AudienceMobileWebServ(), tagStr);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * 求ai1和ai2中符合tagStr的数据数量和
	 * @param ai1
	 * @param ai2
	 * @param tagStr
	 * @return
	 */
	public Map<String,Object> getAppMatchData(AudienceInterface ai1,AudienceInterface ai2,String tagStr){

//		AudienceIosStatsServ aiosss=new AudienceIosStatsServ();
//		AudienceImeiStatsServ aimeiss=new AudienceImeiStatsServ();
		if(isNotNull(ai1)==null){
			return this.getMatchTagData(ai2,tagStr);
		}
		if(isNotNull(ai2)==null){
			return this.getMatchTagData(ai1,tagStr);
		}
		Map<String,int[]> map1=asts.countAudiences2(ai1.getAudienceTagSum(),ai1.getAudienceTagList(),ai1.getAudienceTag(),ai1.getAudienceTagMap(),tagStr);//统计符合条件的人数，并统计每个标签的数量
		Map<String,int[]> map2=asts.countAudiences2(ai2.getAudienceTagSum(),ai2.getAudienceTagList(),ai2.getAudienceTag(),ai2.getAudienceTagMap(),tagStr);//统计符合条件的人数，并统计每个标签的数量
		Map<String,int[]> mergemap = merge(map1,map2,ai1.getRate(),ai2.getRate());//合并两个中间结果
		
//		int iosimeiSamSum=aiosss.getAudienceTagMap().get(Short.parseShort("0"))+aimeiss.getAudienceTagMap().get(Short.parseShort("0"));//ios和imei抽样后数量的和
		int audiSum=ai1.getAudiSum()+ai2.getAudiSum();//当天ai1和ai2库中人数的和
//		float rate=new BigDecimal((iosimeiAudiSum)/((float)1.0*iosimeiSamSum)).setScale(2, BigDecimal.ROUND_HALF_UP).floatValue();//抽样前基数的和/抽样后数量的和
		Map<String,Object> tagMap=asts.getMatchTagMap(audiSum,mergemap);//将所有标签分类
		
		//乘以比例后，就是抽样前的总数，因此直接将audiSum作为样本数传入即可
		asts.initRate(audiSum,tagMap);
//		asts.multiRate(rate,tagMap);
		return put2Map(tagMap);
		
	}
	/**
	 * 求ai1和ai2和ai3中符合tagStr的数据数量和
	 * @param ai1
	 * @param ai2
	 * @param tagStr
	 * @return
	 */
	public Map<String,Object> getMobileMatchData(AudienceInterface ai1,AudienceInterface ai2,AudienceInterface ai3,String tagStr){

//		AudienceIosStatsServ aiosss=new AudienceIosStatsServ();
//		AudienceImeiStatsServ aimeiss=new AudienceImeiStatsServ();
		if(isNotNull(ai1)==null){
			return this.getAppMatchData(ai2,ai3,tagStr);
		}
		if(isNotNull(ai2)==null){
			return this.getAppMatchData(ai1,ai3,tagStr);
		}
		if(isNotNull(ai3)==null){
			return this.getAppMatchData(ai1,ai2,tagStr);
		}
		Map<String,int[]> map1=asts.countAudiences2(ai1.getAudienceTagSum(),ai1.getAudienceTagList(),ai1.getAudienceTag(),ai1.getAudienceTagMap(),tagStr);//统计符合条件的人数，并统计每个标签的数量
		Map<String,int[]> map2=asts.countAudiences2(ai2.getAudienceTagSum(),ai2.getAudienceTagList(),ai2.getAudienceTag(),ai2.getAudienceTagMap(),tagStr);//统计符合条件的人数，并统计每个标签的数量
		Map<String,int[]> temp_mergemap = merge(map1,map2,ai1.getRate(),ai2.getRate());//合并两个中间结果
		Map<String,int[]> map3=asts.countAudiences2(ai3.getAudienceTagSum(),ai3.getAudienceTagList(),ai3.getAudienceTag(),ai3.getAudienceTagMap(),tagStr);//统计符合条件的人数，并统计每个标签的数量
		Map<String,int[]> mergemap = merge(temp_mergemap,map3,1.0f,ai3.getRate());//合并两个中间结果
		
//		int iosimeiSamSum=aiosss.getAudienceTagMap().get(Short.parseShort("0"))+aimeiss.getAudienceTagMap().get(Short.parseShort("0"));//ios和imei抽样后数量的和
		int audiSum=ai1.getAudiSum()+ai2.getAudiSum()+ai3.getAudiSum();//当天ai1和ai2和ai3库中人数的和
//		float rate=new BigDecimal((iosimeiAudiSum)/((float)1.0*iosimeiSamSum)).setScale(2, BigDecimal.ROUND_HALF_UP).floatValue();//抽样前基数的和/抽样后数量的和
		Map<String,Object> tagMap=asts.getMatchTagMap(audiSum,mergemap);//将所有标签分类
		
		//乘以比例后，就是抽样前的总数，因此直接将audiSum作为样本数传入即可
		asts.initRate(audiSum,tagMap);
//		asts.multiRate(rate,tagMap);
		return put2Map(tagMap);
		
	}
	/**
	 * 整理符合标签的人群标签数据，并放入map中，返回
	 */
	public Map<String,Object> getMatchTagData(AudienceInterface ass,String tagStr){
		if(isNotNull(ass)!=null){
//			Map<String,Object> tagMap=getMatchData(ass,tagStr);
			Map<String,int[]> mapCountAudi=asts.countAudiences2(ass.getAudienceTagSum()
					,ass.getAudienceTagList(),ass.getAudienceTag(),ass.getAudienceTagMap(),tagStr);//统计符合条件的人数，并统计每个标签的数量
			Map<String,Object> tagMap = asts.getMatchTagMap(ass.getAudiSum(),mapCountAudi);//将所有标签分类
			asts.initRate(ass.getAudienceTagMap().get(Short.parseShort("0")),tagMap);
			asts.multiRate(ass.getRate(),tagMap);
			return put2Map(tagMap);
		}
		return null;
	}
	public Object isNotNull(AudienceInterface ass){
		if(ass.getAudienceTag() != null&&ass.getAudienceTag().length > 0){
			return new Object();
		}
		return null;
	}
//	/**
//	 * 计算出符合条件的人数和各个标签的统计结果
//	 */
//	public Map<String,Object> getMatchData(AudienceInterface ass,String tagStr){
//		
//		Map<String,Object> tagMap = asts.getMatchTagMap(ass.getAudiSum(),mapCountAudi);//将所有标签分类
//		asts.initRate(ass.getAudienceTagMap().get(Short.parseShort("0")),tagMap);
//		asts.multiRate(ass.getRate(),tagMap);
//		return tagMap;
//	}
	/**
	 * 结果集整理为所需格式
	 */
	public Map<String,Object> put2Map(Map<String,Object> tagMap){
		
		Map<String,Object> map=new HashMap<String,Object>();
		
		map.put("tagSum","标签数量");
		map.put("tagRate","标签占比");
		map.put("tgiRate","TGI");
		
		map.put("matchSum", tagMap.get("tagMatchSum"));
		map.put("audiTagData",tagMap);
		
		StringBuilder sexResult = asts.InitBar(tagMap,"sex");
		map.put("sexAudiMapBar",sexResult.toString().substring(0, sexResult.toString().length() - 1));
		
		StringBuilder ageResult = asts.InitBar(tagMap,"age");
		map.put("ageAudiMapBar",ageResult.toString().substring(0, ageResult.toString().length() - 1));
		
		StringBuilder incomeResult = asts.InitBar(tagMap,"income");
		map.put("incomeAudiMapBar",incomeResult.toString().substring(0, incomeResult.toString().length() - 1));
		
		StringBuilder educationResult = asts.InitBar(tagMap,"education");
		map.put("educationAudiMapBar",educationResult.toString().substring(0, educationResult.toString().length() - 1));
		
		StringBuilder professionResult = asts.InitBar(tagMap,"profession");
		map.put("professionAudiMapBar",professionResult.toString().substring(0, professionResult.toString().length() - 1));
		
		StringBuilder industryResult = asts.InitBar(tagMap,"industry");
		map.put("industryAudiMapBar",industryResult.toString().substring(0, industryResult.toString().length() - 1));
		
		StringBuilder lifePeriodResult = asts.InitBar(tagMap,"lifePeriod");
		map.put("lifePeriodAudiMapBar",lifePeriodResult.toString().substring(0, lifePeriodResult.toString().length() - 1));
		
		StringBuilder attentionResult = asts.InitBar(tagMap,"attention");
		map.put("attentionAudiMapBar",attentionResult.toString().substring(0, attentionResult.toString().length() - 1));
		
		StringBuilder buyResult = asts.InitBar(tagMap,"buy");
		map.put("buyAudiMapBar",buyResult.toString().substring(0, buyResult.toString().length() - 1));
		
//		map.put("audiStatsToolRate", audienceStatisticService.getRate());
		map.put("audiStatsToolSum", tagMap.get("baseSum"));
		
		return map;
	}
	
	/**
	 * 合并ios和imei的中间结果
	 * 
	 */
	public Map<String,int[]> merge(Map<String,int[]> iosMap,Map<String,int[]> imeiMap,float iosRate,float imeiRate){
		Map<String,int[]> mergeMap=new HashMap<String,int[]>();
//		List<AudienceCategory> audiList=asts.getAudienceList();//从数据库中查询到的人群标签列表
		for (Map.Entry<String, int[]> entry : iosMap.entrySet()) {
			String name=entry.getKey();
			int[] value=entry.getValue();
			int[] iosAddimei=new int[]{asts.rMultyInt(iosRate,value[0])+asts.rMultyInt(imeiRate,imeiMap.get(name)[0]),asts.rMultyInt(iosRate,value[1])+asts.rMultyInt(imeiRate,imeiMap.get(name)[1])};
			mergeMap.put(name, iosAddimei);
		}
		return mergeMap;
	}
	/**
	 * 获取符合条件tagStr和类型type的人数和总基数
	 * type : PC , IOS , Imei , App, Moweb, Mobile
	 */
	public AudienceNum getMatchNum(String tagStr,String type){
		try{
			if("PC".equalsIgnoreCase(type)){
				return this.matchNum(new AudienceStatisticService(),tagStr);
			}else if("IOS".equalsIgnoreCase(type)){
				return this.matchNum(new AudienceIosStatsServ(),tagStr);
			}else if("Imei".equalsIgnoreCase(type)){
				return this.matchNum(new AudienceImeiStatsServ(),tagStr);
			}else if("App".equalsIgnoreCase(type)){
				AudienceNum an1=this.matchNum(new AudienceIosStatsServ(),tagStr);
				AudienceNum an2=this.matchNum(new AudienceImeiStatsServ(),tagStr);
				return this.numberMerge(an1, an2);
			}else if("Moweb".equalsIgnoreCase(type)){
				return this.matchNum(new AudienceMobileWebServ(),tagStr);
			}else if("Mobile".equalsIgnoreCase(type)){
//				return this.appMatchNum(tagStr);
				AudienceNum an1=this.matchNum(new AudienceIosStatsServ(),tagStr);
				AudienceNum an2=this.matchNum(new AudienceImeiStatsServ(),tagStr);
				AudienceNum an3=this.matchNum(new AudienceMobileWebServ(),tagStr);
				return this.numberMerge(this.numberMerge(an1, an2), an3);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 获取符合条件tagStr和类型type的人数和总基数(总库人数和总库基数)
	 * type : PC , IOS , Imei , App, Moweb, Mobile
	 */
	public AudienceNum getLibMatchNum(String tagStr,String type){
		try{
			if("PC".equalsIgnoreCase(type)){
				return this.libMatchNum(new AudienceStatisticService(), tagStr);
			}else if("IOS".equalsIgnoreCase(type)){
				return this.libMatchNum(new AudienceIosStatsServ(), tagStr);
			}else if("Imei".equalsIgnoreCase(type)){
				return this.libMatchNum(new AudienceImeiStatsServ(), tagStr);
			}else if("App".equalsIgnoreCase(type)){
				AudienceNum an1=this.libMatchNum(new AudienceIosStatsServ(),tagStr);
				AudienceNum an2=this.libMatchNum(new AudienceImeiStatsServ(),tagStr);
				return this.numberMerge(an1, an2);
			}else if("Moweb".equalsIgnoreCase(type)){
				return this.libMatchNum(new AudienceMobileWebServ(), tagStr);
			}else if("Mobile".equalsIgnoreCase(type)){
				AudienceNum an1=this.libMatchNum(new AudienceIosStatsServ(),tagStr);
				AudienceNum an2=this.libMatchNum(new AudienceImeiStatsServ(),tagStr);
				AudienceNum an3=this.libMatchNum(new AudienceMobileWebServ(),tagStr);
				return this.numberMerge(this.numberMerge(an1, an2), an3);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 得到抽样库中，ai满足tagStr条件的人数和
	 * 
	 */
	private AudienceNum matchNum(AudienceInterface ai,String tagStr){
		AudienceNum arr=new AudienceNum();
		arr.matchNum=asts.getTotalNum(ai.getRate(), ai.getAudienceTag(), ai.getAudienceTagMap(), tagStr);
		arr.libNum=ai.getAudiSum();
		return arr;
	}
	/**
	 * 得到总库中，ai匹配tagStr条件的人数和总库基数
	 * 
	 */
	private AudienceNum libMatchNum(AudienceInterface ai,String tagStr){
//		AudienceNum an=this.matchNum(ai,tagStr);//得到一天的量匹配的人数
		AudienceNum newAn=new AudienceNum();
		long matchNum=asts.getTotalNum(ai.getRate(), ai.getAudienceTag(), ai.getAudienceTagMap(), tagStr);
//		System.out.println("中间结果："+matchNum);
		newAn.matchNum=asts.rMultyInt(ai.getLibRate(),matchNum);//一天的matchNum乘以与总库的比例，得到在总库中的匹配人数
		newAn.libNum=ai.getLibNum();
		return newAn;
	}
	/**
	 * 得到ai1、ai2的人数和，以及ai1、ai2总数和
	 * 
	 */
	private AudienceNum numberMerge(AudienceNum ai1, AudienceNum ai2){
		AudienceNum an=new AudienceNum();
		an.matchNum=ai1.matchNum+ai2.matchNum;
		an.libNum=ai1.libNum+ai2.libNum;
		return an;
	}
//	@SuppressWarnings("unchecked")
//	public Map<String,Object> mergeIosImei(Map<String,Object> ios,Map<String,Object> imei,float rate){
//		Map<String,Object> resultMap=new HashMap<String,Object>();
//		for(Map.Entry<String, Object> entry:ios.entrySet()){
//			String key=entry.getKey();
//			Object value=entry.getValue();
//			if (value instanceof ArrayList<?>) {
//				if(imei.containsKey(key)){
//					List<AudienceStatisticModel> list=(List<AudienceStatisticModel>)value;
//					List<AudienceStatisticModel> imeiList=(List<AudienceStatisticModel>)imei.get(key);
//					List<AudienceStatisticModel> tempList=new ArrayList<AudienceStatisticModel>();
//					int imeiLen=imeiList.size();
//					for (AudienceStatisticModel audienceStatisticModel : list) {
//						String name=audienceStatisticModel.getAudiName();
//						int count=0;
//						for(AudienceStatisticModel audienceStatisticModel2 : imeiList){
//							if(name.equals(audienceStatisticModel2.getAudiName())){
//								AudienceStatisticModel asm=new AudienceStatisticModel();
//								asm.setAudiName(name);
////								asm.setTagRate(audienceStatisticModel.getTagRate()+audienceStatisticModel2.getTagRate());
//								asm.setTagSum(audienceStatisticModel.getTagSum()+audienceStatisticModel2.getTagSum());
//								asm.setTagTotalSum(audienceStatisticModel.getTagTotalSum()+audienceStatisticModel2.getTagTotalSum());
////								asm.setTgiRate(audienceStatisticModel.getTgiRate()+audienceStatisticModel2.getTgiRate());
//								tempList.add(asm);
//								break;
//							}else{
//								count++;
//							}
//						}
//						if(count==imeiLen){
//							tempList.add(audienceStatisticModel);
//						}
//					}
//					resultMap.put(key, tempList);
//				}else{
//					resultMap.put(key, value);
//				}
//			}else if(value instanceof HashMap<?,?>){
//				if(imei.containsKey(key)){
//					Map<String,AudienceStatisticModel> tagMap=(Map<String,AudienceStatisticModel>)value;
//					Map<String,AudienceStatisticModel> imeiMap=(Map<String,AudienceStatisticModel>)imei.get(key);
//					Map<String,AudienceStatisticModel> tempMap=new HashMap<String,AudienceStatisticModel>();
//					for (Map.Entry<String, AudienceStatisticModel> entry2:tagMap.entrySet()) {
//						String name=entry2.getKey();
//						AudienceStatisticModel entry2Value=entry2.getValue();
//						if(imeiMap.containsKey(name)){
//							AudienceStatisticModel imeiValue=imeiMap.get(name);
//							AudienceStatisticModel asm=new AudienceStatisticModel();
//							asm.setAudiName(name);
//							asm.setTagSum(entry2Value.getTagSum()+imeiValue.getTagSum());
//							asm.setTagTotalSum(entry2Value.getTagTotalSum()+imeiValue.getTagTotalSum());
////							asm.setTagRate(entry2Value.getTagRate()+imeiValue.getTagRate());
////							asm.setTgiRate(entry2Value.getTgiRate()+imeiValue.getTgiRate());
//							tempMap.put(name, asm);
//						}else{
//							tempMap.put(name, entry2Value);
//						}
//					}
//					resultMap.put(key, tempMap);
//				}else{
//					resultMap.put(key, value);
//				}
//			}else if(value instanceof Integer){
//				if(imei.containsKey(key)){
//					int sum=asts.rMultyInt(rate,((int) imei.get(key)+(int)ios.get(key)));
//					resultMap.put(key, sum);
//				}else{
//					resultMap.put(key, value);
//				}
//			}
//		}
//		
//		return resultMap;
//	}
}
