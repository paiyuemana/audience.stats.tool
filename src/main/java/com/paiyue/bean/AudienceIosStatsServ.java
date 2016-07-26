package com.paiyue.bean;

import java.util.List;
import java.util.Map;

import com.paiyue.bean.AudienceInterface;

public class AudienceIosStatsServ implements AudienceInterface {

	private static long[][] audienceTag;//人群库cookie具有的所有标签
	
	private static Map<Short,Integer> audienceTagMap;//map key 标签id，map value 标签对应的数字
	
	private static List<Short> audienceTagList;//与map同，下标为标签对应的数字，用于直接通过下标获取标签id
	
	private static Map<Short,Integer> audienceTagSum;//记录标签id在人群库中的数量

	private static float rate;//audiSum与抽样后数量的比值
	
	private static int audiSum;//ios对应一天数据的总数
	
	private static float libRate;// libNum/一天的cookieid数量 比值
	private static long libNum=48697751;//ios 总人群库中人数
	
//	private static String tagSumPath = "/iosdata/countTagSum.txt";//标签数量统计文件
//	private static String audiIdFlagPath = "/iosdata/audienceCookieFlag.txt";//人群id和标签文件
//	private static String ratePath = "/iosdata/iosrate";//比例和基数文件
//	private static String oldTagSumPath = "/oldios/countTagSum.txt";//标签数量统计文件
//	private static String oldAudiIdFlagPath = "/oldios/audienceCookieFlag.txt";//人群id和标签文件
//	private static String oldRatePath = "/oldios/iosrate";//比例和基数文件
	private String oldIdMatchedPath = "/user/na.ma/oldIosSample2";//hadoop上old id-标签的文件路径
	private String oldTagSumPath = "/user/na.ma/oldtagSum-ios";//hadoop上old标签数量的路径
	private String idMatched_path="/user/na.ma/iosSample2";//hadoop上的ios匹配后的文件路径
	private String idTagSum_path="/user/na.ma/tagSum-ios";//hadoop上的ios标签数量的路径
	private String idMatched_key="matched-iosid";
	private String idDay_key="day-iosid";
	
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
//		AudienceIosStatsServ.tagSumPath = countTagSum;
//	}
//
//	public String getAudiIdFlagPath() {
//		return audiIdFlagPath;
//	}
//
//	public void setAudiIdFlagPath(String audiCooFlagPath) {
//		AudienceIosStatsServ.audiIdFlagPath = audiCooFlagPath;
//	}
//
//	public String getRatePath() {
//		return ratePath;
//	}
//
//	public void setRatePath(String ratePath) {
//		AudienceIosStatsServ.ratePath = ratePath;
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
//		AudienceIosStatsServ.oldAudiIdFlagPath = oldAudiIdFlagPath;
//	}
//	public String getOldRatePath() {
//		return oldRatePath;
//	}
//	public void setOldRatePath(String oldRatePath) {
//		AudienceIosStatsServ.oldRatePath = oldRatePath;
//	}
	public long getLibNum() {
		return libNum;
	}
	public void setLibNum(long libNum) {
		AudienceIosStatsServ.libNum = libNum;
	}
	public float getLibRate() {
		return libRate;
	}
	public void setLibRate(float libRate) {
		AudienceIosStatsServ.libRate = libRate;
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