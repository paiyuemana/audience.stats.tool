package com.paiyue.bean;

public class AudienceCategory {
	/**
	 * 主键
	 */
	protected Short id;
	/**
	 * 节点名称
	 */
	private String name;

	/**
	 * 节点名称路径,例如: 个人关注/资讯、新闻/各行业资讯/法律
	 */
	private String rawData;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Short getId() {
		return id;
	}

	public void setId(Short id) {
		this.id = id;
	}
	
	public String getRawData() {
		return rawData;
	}

	public void setRawData(String rawData) {
		this.rawData = rawData;
	}

}
