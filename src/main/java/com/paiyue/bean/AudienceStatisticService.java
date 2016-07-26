package com.paiyue.bean;

import java.util.List;
import java.util.Map;

import com.paiyue.bean.AudienceInterface;

public class AudienceStatisticService  implements AudienceInterface {
	
	private static long[][] audienceTag;//人群库cookie具有的所有标签
	
	private static Map<Short,Integer> audienceTagMap;//map key 标签id，map value 标签对应的数字
	
	private static List<Short> audienceTagList;//与map同，下标为标签对应的数字，用于直接通过下标获取标签id
	
	private static Map<Short,Integer> audienceTagSum;//记录标签id在人群库中的数量

	private static float rate;//audiSum与抽样后数量的比值
	
	private static int audiSum;//cookieid对应一天数据的总数
	
	private static float libRate;// libNum/一天的cookieid数量 比值
	private static long libNum=774065014;//cookie id 人群库中的人数
	
//	private static String tagSumPath = "/data/countTagSum.txt";//标签数量统计文件
//	private static String audiIdFlagPath = "/data/audienceCookieFlag.txt";//人群id和标签文件
//	private static String ratePath = "/data/rate";//比例和基数文件
	private String oldIdMatchedPath = "/user/na.ma/oldCookieSample2";//hadoop上old id-标签的文件路径
	private String oldTagSumPath = "/user/na.ma/oldtagSum-cookie";//hadoop上old标签数量的路径
//	private static String oldAudiIdFlagPath = "/old/audienceCookieFlag.txt";//人群id和标签文件
//	private static String oldRatePath = "/old/rate";//比例和基数文件

	private String idMatched_path="/user/na.ma/cookieSample2";//hadoop上的cookie id匹配后的文件路径
	private String idTagSum_path="/user/na.ma/tagSum-cookie";//hadoop上的cookie id标签数量的路径
	private String idMatched_key="matched-cookieid";
	private String idDay_key="day-cookieid";
	
	public List<Short> getAudienceTagList(){
		return audienceTagList;
	}
	public void setAudienceTagList(List<Short> list){
		audienceTagList=list;
	}
	public Map<Short, Integer> getAudienceTagSum(){
		return audienceTagSum;
	}
	public void setAudienceTagSum(Map<Short, Integer> map){
		audienceTagSum=map;
	}
	
	public long[][] getAudienceTag(){
		return audienceTag;
	}
	public void setAudienceTag(long[][] tag){
		audienceTag=tag;
	}
	public  Map<Short,Integer> getAudienceTagMap(){
		return audienceTagMap;
	}
	public void setAudienceTagMap(Map<Short,Integer> map){
		audienceTagMap=map;
	}
	public float getRate(){
		return rate;
	}
	public void setRate(float r){
		rate=r;
	}
	public int getAudiSum(){
		return audiSum;
	}
	public void setAudiSum(int as){
		audiSum=as;
	}
//	public String getTagSumPath() {
//		return tagSumPath;
//	}
//
//	public void setTagSumPath(String countTagSum) {
//		AudienceStatisticService.tagSumPath = countTagSum;
//	}
//
//	public String getAudiIdFlagPath() {
//		return audiIdFlagPath;
//	}
//
//	public void setAudiIdFlagPath(String audiCooFlagPath) {
//		AudienceStatisticService.audiIdFlagPath = audiCooFlagPath;
//	}
//
//	public String getRatePath() {
//		return ratePath;
//	}
//
//	public void setRatePath(String ratePath) {
//		AudienceStatisticService.ratePath = ratePath;
//	}
	public String getOldTagSumPath() {
		return oldTagSumPath;
	}
	public void setOldTagSumPath(String oldTagSumPath) {
		this.oldTagSumPath = oldTagSumPath;
	}
//	public String getOldAudiIdFlagPath() {
//		return oldAudiIdFlagPath;
//	}
//	public void setOldAudiIdFlagPath(String oldAudiIdFlagPath) {
//		AudienceStatisticService.oldAudiIdFlagPath = oldAudiIdFlagPath;
//	}
//	public String getOldRatePath() {
//		return oldRatePath;
//	}
//	public void setOldRatePath(String oldRatePath) {
//		AudienceStatisticService.oldRatePath = oldRatePath;
//	}
	public float getLibRate() {
		return libRate;
	}
	public void setLibRate(float libRate) {
		AudienceStatisticService.libRate = libRate;
	}
	public long getLibNum() {
		return libNum;
	}
	public void setLibNum(long libNum) {
		AudienceStatisticService.libNum = libNum;
	}
	public String getIdMatched_path() {
		return idMatched_path;
	}
	public void setIdMatched_path(String idMatched_path) {
		this.idMatched_path = idMatched_path;
	}
	public String getIdTagSum_path() {
		return idTagSum_path;
	}
	public void setIdTagSum_path(String idTagSum_path) {
		this.idTagSum_path = idTagSum_path;
	}
	public String getOldIdMatchedPath() {
		return oldIdMatchedPath;
	}
	public void setOldIdMatchedPath(String oldIdMatchedPath) {
		this.oldIdMatchedPath = oldIdMatchedPath;
	}
	public String getIdMatched_key() {
		return idMatched_key;
	}
	public String getIdDay_key() {
		return idDay_key;
	}
}

