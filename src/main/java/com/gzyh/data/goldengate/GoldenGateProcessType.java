package com.gzyh.data.goldengate;


public enum GoldenGateProcessType {
	IMPALA,
	KUDU;
	public static GoldenGateProcessType fromString(String str) {
		return Enum.valueOf(GoldenGateProcessType.class, str.toUpperCase());
	}
}
