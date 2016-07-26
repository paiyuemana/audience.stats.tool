package com.paiyue.bean;

import java.util.List;
import java.util.Map;

public interface AudienceInterface {
	
	public long[][] getAudienceTag();
	public List<Short> getAudienceTagList();
	public void setAudienceTagList(List<Short> list);
	public Map<Short, Integer> getAudienceTagSum();
	public void setAudienceTagSum(Map<Short, Integer> map);
	public void setAudienceTag(long[][] tag);
	public  Map<Short,Integer> getAudienceTagMap();
	public void setAudienceTagMap(Map<Short,Integer> map);
	public float getRate();
	public void setRate(float r);
	public int getAudiSum();
	public void setAudiSum(int as);
//	public String getTagSumPath();
//	public void setTagSumPath(String countTagSum);
//	public String getAudiIdFlagPath();
//	public void setAudiIdFlagPath(String audiCooFlagPath);
//	public String getRatePath();
//	public void setRatePath(String ratePath);
	public String getOldTagSumPath();
	public void setOldTagSumPath(String oldTagSumPath);
	public String getOldIdMatchedPath();
	public void setOldIdMatchedPath(String oldIdMatchedPath);
//	public String getOldAudiIdFlagPath();
//	public void setOldAudiIdFlagPath(String oldAudiIdFlagPath);
//	public String getOldRatePath();
//	public void setOldRatePath(String oldRatePath);
	public float getLibRate();
	public void setLibRate(float libRate);
	public long getLibNum();
	public void setLibNum(long libNum);
	public String getIdMatched_path();
	public void setIdMatched_path(String idMatched_path) ;
	public String getIdTagSum_path();
	public void setIdTagSum_path(String idTagSum_path) ;
	public String getIdMatched_key();
	public String getIdDay_key();
}
