package com.gzyh.data.type;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;


public class OracleType2DataType implements DBType2DataType{
	private Map<Integer,Integer> DBType2DataType_map = new HashMap<Integer, Integer>(0);
	public OracleType2DataType(){
		DBType2DataType_map.put(1, Types.VARCHAR);
		DBType2DataType_map.put(2, Types.DOUBLE);
		DBType2DataType_map.put(8, Types.TIMESTAMP);
		DBType2DataType_map.put(12, Types.TIMESTAMP);
		DBType2DataType_map.put(96, Types.VARCHAR);
		DBType2DataType_map.put(100, Types.FLOAT);
		DBType2DataType_map.put(101, Types.DOUBLE);
		DBType2DataType_map.put(112, Types.VARCHAR);
		DBType2DataType_map.put(187, Types.TIMESTAMP);
	}

	@Override
	public int getDataType(int native_type) {
		if(!DBType2DataType_map.containsKey(native_type)){
			return Types.NULL;
		}else{
			return DBType2DataType_map.get(native_type);			
		}
	}

}
