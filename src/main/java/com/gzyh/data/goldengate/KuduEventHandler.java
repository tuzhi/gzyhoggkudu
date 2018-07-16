package com.gzyh.data.goldengate;

import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import oracle.goldengate.datasource.DsConfiguration;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.adapt.Col;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.adapt.Tx;
import oracle.goldengate.datasource.meta.DsMetaData;

import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gzyh.data.type.DBType2DataType;
import com.gzyh.data.type.MysqlType2DataType;
import com.gzyh.data.type.OracleType2DataType;

public class KuduEventHandler extends BaseEventHandler implements IGzyhOggProcess{
	private static final Logger log = LoggerFactory.getLogger(KuduEventHandler.class);
	private KuduClient client = null;
	private KuduSession session = null;
	private DBType2DataType typeMapHandler = null;
	//外部IMPALA参数
	private String kudu_master;
	private String flush_mode;
	private int manual_flush_batch = 500;
	//
	private Map<String,KuduTable> tableMap = new HashMap<String,KuduTable>();

	public void setKudu_master(String kudu_master) {
		this.kudu_master = kudu_master;
	}

	public void setFlush_mode(String flush_mode) {
		this.flush_mode = flush_mode;
	}

	public void setManual_flush_batch(int manual_flush_batch) {
		this.manual_flush_batch = manual_flush_batch;
	}

	@Override
	public void initHandler(DsConfiguration conf, DsMetaData metaData) throws Exception {
		if(client == null){
			log.info("kudu_master:" + kudu_master + ",flush_mode:" + flush_mode+",manual_flush_batch:" + manual_flush_batch +",schemaName:" + schemaName);
			client = new KuduClient.KuduClientBuilder(kudu_master).build();
			session = client.newSession();
			if(from_oracle){
				typeMapHandler = new OracleType2DataType();				
			}else{
				typeMapHandler = new MysqlType2DataType();
			}
		}
	}

	private void setObject(PartialRow row,Col column ,boolean isAfter) throws Exception{
		int columnIndex = column.getIndex();
		if(isDebugEnabled){
			log.info("setObject:" + columnIndex+ ",value:"+column+",type:"+column.getDataType() + "|" + column.getDataType().getJDBCType() + "|"
					+column.getDataType().getGGDataType()+"|"
					+column.getDataType().getGGDataSubType()+"|"
					+column.getMeta().getColumnDataType() + "|"
					+column.getMeta().getNativeDataType() + "|");
		}
		if(column.isNull()){
			row.setNull(columnIndex);
		}else{
			String strvalue = isAfter?column.getAfterValue():column.getBeforeValue();
			int native_type = column.getMeta().getNativeDataType();
			int targetJDBCType = typeMapHandler.getDataType(native_type);
			switch(targetJDBCType){
			case Types.TINYINT:
				row.addByte(columnIndex, Byte.valueOf(strvalue));
				break;
			case Types.INTEGER:
				row.addInt(columnIndex, Integer.valueOf(strvalue));
				break;
			case Types.BIGINT:
				row.addLong(columnIndex, Long.valueOf(strvalue));
				break;
			case Types.FLOAT:
				row.addFloat(columnIndex, Float.valueOf(strvalue));
				break;
			case Types.DOUBLE:
				row.addDouble(columnIndex, Double.valueOf(strvalue));
				break;
			case Types.VARCHAR:
				row.addString(columnIndex, strvalue);
				break;
			case Types.TIMESTAMP:
//				int native_type = column.getMeta().getNativeDataType();
//				switch(native_type){
//				case 7://timestamp 1970-01-01 00:00:00.000000
//				case 12://date 1970-01-01 00:00:00
//					if(tsvalue.charAt(10) == ':'){
//						char[] charArray = tsvalue.toCharArray();
//						charArray[10] = ' ';
//						tsvalue = new String(charArray);
//					}
//					Timestamp ts = Timestamp.valueOf(tsvalue);
//					row.addLong(columnIndex, ts.getTime() * 1000 + (ts.getNanos()/1000- ts.getNanos()/1000000*1000));
//					break;
//				case 10://1970-01-01
//					Date dspart1 = Date.valueOf(tsvalue);
//					row.addLong(columnIndex, dspart1.getTime());
//					break;
//				case 11://00:00:00
//					row.addString(columnIndex, tsvalue);
//					break;
//				default:
//					break;
//				}
					row.addString(columnIndex, strvalue);
					break;
				default:
					log.error("JDBCType not support:" + column.getDataType().getJDBCType()+",native type:" + column.getMeta().getNativeDataType());
					throw new Exception("JDBCType not support:" + column.getDataType().getJDBCType()+",native type:" + column.getMeta().getNativeDataType());
				}
			}
		}

		@Override
		public Status process(Tx tx, Op op) throws Exception {
			Status status = Status.OK;
			if(op.getNumColumns()==0){
				return status;
			}
			//查询case sensitive表名，Impala接口忽略大小写，而Kudu大小写敏感
			String tableName = getDstTableName(op);
			if(!tableMap.containsKey(tableName)){
				KuduTable table = client.openTable(tableName);
				tableMap.put(tableName, table);
			}
			KuduTable table = tableMap.get(tableName);
			boolean flag = false;
			if(flag){
				return status;
			}
			OperationResponse response = null;
			if(op.getOperationType().isInsert()){
				Insert insert = table.newInsert();
				PartialRow row = insert.getRow();
				for(Col column : op){
					if(!column.hasAfter() && !column.hasBefore()){
						continue;
					}
					setObject(row,column,true);
				}
				response = session.apply(insert);
			}else if(op.getOperationType().isDelete()){
				Delete delete = table.newDelete();
				PartialRow row = delete.getRow();
				for(Col column : op){
					if(!column.hasAfter() && !column.hasBefore()){
						continue;
					}
					if(column.getMeta().isKeyCol()){
						setObject(row,column,false);
					}
				}
				response = session.apply(delete);
			}else if(op.getOperationType().isUpdate()){
				Update update = table.newUpdate();
				PartialRow row = update.getRow();
				for(Col column : op){
					if(!column.hasAfter() && !column.hasBefore()){
						continue;
					}
					setObject(row,column,true);
				}
				response = session.apply(update);
			}
			if(isDebugEnabled){
				log.info("response:" + response.getRowError());
			}
			return status;
		}

		@Override
		public void destroyHandler() throws Exception {
			session.close();
			client.close();
		}

		private String getDstTableName(Op op){
			return "impala::" + (schemaName == null? (op.getTableName().getSchemaName() + "_kudu"):schemaName).toLowerCase() + "." + op.getTableName().getShortName().toLowerCase();
		}

		public static void main(String[] args) throws Exception{
			String str = "2015-10-26:17:30:00.000000020";
			//		str = "2015-10-26:17:30:00";
			if(str.length()>=19 && str.charAt(10) == ':'){
				char[] charArray = str.toCharArray();
				charArray[10] = ' ';
				str = new String(charArray);
			}

			//		Pattern p = Pattern.compile("^(\\d\\d\\d\\d-\\d\\d-\\d\\d):(\\d\\d:\\d\\d:\\d\\d.*)");
			//		Matcher m = p.matcher(str);
			//		m.find();
			//		String kk = m.group(1);
			//		String kkk = m.group(2);

			Timestamp ts = Timestamp.valueOf(str);
			//		tem = "2015-10-26:17:30:00";
			//		DateFormat f = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss[.f...]");
			//		Date obj = f.parse(tem);
			//		long kk = obj.getTime();
			//		Timestamp ts = new Timestamp(kk);

			//		
			//		tem = "2015-10-26:17:30:00";
			//		f = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
			//		obj = f.parse(tem);
		}


	}
