package com.gzyh.data.type;

import org.apache.kudu.client.PartialRow;


public class OracleType2DataType implements DBType2DataType{

	@Override
	public void getDataType(PartialRow row, int columnIndex, int native_type,
			String strvalue) {
		switch(native_type){
		case 100:
			row.addFloat(columnIndex, Float.valueOf(strvalue));
			break;
		default:
			row.addDouble(columnIndex,Double.valueOf(strvalue));
			break;
		}
	}

}
