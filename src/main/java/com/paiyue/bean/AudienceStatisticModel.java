package com.paiyue.bean;
public class AudienceStatisticModel {
	private int audiId;
	private String audiName;
	private int tagSum;
	private float tagRate;
	private int tagTotalSum;
	private float tgiRate;
	public AudienceStatisticModel(){
		tagSum=0;
		tagRate=0.0f;
		setTgiRate(0.0f);
		audiName="";
	}
	public int getAudiId() {
		return audiId;
	}
	public void setAudiId(int audiId) {
		this.audiId = audiId;
	}
	public String getAudiName() {
		return audiName;
	}
	public void setAudiName(String audiName) {
		this.audiName = audiName;
	}
	public int getTagSum() {
		return tagSum;
	}
	public void setTagSum(int tagSum) {
		this.tagSum = tagSum;
	}
	public float getTagRate() {
		return tagRate;
	}
	public void setTagRate(float tagRate) {
		this.tagRate = tagRate;
	}
	public float getTgiRate() {
		return tgiRate;
	}
	public void setTgiRate(float tgiRate) {
		this.tgiRate = tgiRate;
	}
	public int getTagTotalSum() {
		return tagTotalSum;
	}
	public void setTagTotalSum(int tagTotalSum) {
		this.tagTotalSum = tagTotalSum;
	}
}
