package com.gzyh.data.type;

import org.apache.kudu.client.PartialRow;




public class MysqlType2DataType implements DBType2DataType{

	@Override
	public void getDataType(PartialRow row, int columnIndex, int native_type,
			String strvalue) {
		switch(native_type){
		case 1:
			row.addByte(columnIndex, Byte.valueOf(strvalue));
			break;
		case 2:
		case 3:
			row.addInt(columnIndex, Integer.valueOf(strvalue));
			break;
		case 8:
			row.addLong(columnIndex, Long.valueOf(strvalue));
			break;
		default:
			row.addDouble(columnIndex,Double.valueOf(strvalue));
			break;
		}
	}
}
