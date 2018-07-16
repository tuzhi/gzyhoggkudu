package com.gzyh.data.type;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;


public class MysqlType2DataType implements DBType2DataType{
	private Map<Integer,Integer> DBType2DataType_map = new HashMap<Integer, Integer>(0);
	public MysqlType2DataType(){
		DBType2DataType_map.put(1, Types.TINYINT);
		DBType2DataType_map.put(2, Types.INTEGER);
		DBType2DataType_map.put(3, Types.INTEGER);
		DBType2DataType_map.put(4, Types.DOUBLE);
		DBType2DataType_map.put(5, Types.DOUBLE);
		DBType2DataType_map.put(7, Types.TIMESTAMP);
		DBType2DataType_map.put(8, Types.BIGINT);
		DBType2DataType_map.put(10, Types.TIMESTAMP);
		DBType2DataType_map.put(11, Types.TIMESTAMP);
		DBType2DataType_map.put(12, Types.TIMESTAMP);
		DBType2DataType_map.put(246, Types.DOUBLE);
		DBType2DataType_map.put(252, Types.VARCHAR);
		DBType2DataType_map.put(253, Types.VARCHAR);
		DBType2DataType_map.put(254, Types.VARCHAR);
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
