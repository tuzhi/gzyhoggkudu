package com.gzyh.data.type;

import org.apache.kudu.client.PartialRow;

public interface DBType2DataType {
	public void getDataType(PartialRow row,int columnIndex,int native_type,String strvalue);
}
