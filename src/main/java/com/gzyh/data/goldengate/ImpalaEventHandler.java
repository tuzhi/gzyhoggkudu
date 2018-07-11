package com.gzyh.data.goldengate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import oracle.goldengate.datasource.DsConfiguration;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.adapt.Col;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.adapt.Tx;
import oracle.goldengate.datasource.meta.DsMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.jdbc41.DataSource;

public class ImpalaEventHandler extends BaseEventHandler implements IGzyhOggProcess{
	private static final Logger log = LoggerFactory.getLogger(ImpalaEventHandler.class);
	private Connection conn = null;
	//外部IMPALA参数
	private String impala_url;
	private String impala_driver_class;// = "com.cloudera.impala.jdbc41.Driver";

	public void setImpala_url(String impala_url) {
		this.impala_url = impala_url;
	}
	public void setImpala_driver_class(String impala_driver_class) {
		this.impala_driver_class = impala_driver_class;
	}

	//内部变量
	private Map<String,PreparedStatement> tableName_insertstat = new HashMap<String, PreparedStatement>();
	private Map<String,PreparedStatement> tableName_updatestat = new HashMap<String, PreparedStatement>();
	private Map<String,PreparedStatement> tableName_deletestat = new HashMap<String, PreparedStatement>();

	@Override
	public void initHandler(DsConfiguration conf, DsMetaData metaData) throws Exception {
		if(conn == null){
			log.info("impala_driver_class:" + impala_driver_class + ",impala_url:" + impala_url+",schemaName:" + schemaName);

			Class.forName(impala_driver_class);
			DataSource ds = new com.cloudera.impala.jdbc41.DataSource();
			ds.setURL(impala_url);
			conn = ds.getConnection();
		}
	}
	
	private Object getAfterOrBeforeObject(Col column,boolean isAfter){
		Object obj;
		if(column.getDataType().getJDBCType() == Types.TIMESTAMP){
			String str = isAfter?column.getAfterValue():column.getBeforeValue();
			if(str.length()>=19 && str.charAt(10) == ':'){
				char[] charArray = str.toCharArray();
				charArray[10] = ' ';
				str = new String(charArray);
			}
			obj = Timestamp.valueOf(str);
		}else{
			obj = isAfter?column.getAfterObject():column.getBeforeObject();
		}
		return obj;
	}
	
	@Override
	public Status process(Tx tx, Op op) throws SQLException {
		Status status = Status.OK;
		if(op.getNumColumns()==0){
			return status;
		}
		if(op.getOperationType().isInsert()){
			String key = getPreparedStatementMapKey(op);
			PreparedStatement insertState = get_insert_PreparedStatement(key,op);
			StringBuffer loginfobuff = new StringBuffer();
			for(Col column : op){
				if(!column.hasAfter() && !column.hasBefore()){
					continue;
				}
				Object obj = getAfterOrBeforeObject(column,true);
				
				insertState.setObject(column.getIndex()+1, obj);
				loginfobuff.append(obj + "|");
			}
			insertState.executeUpdate();
			if (isDebugEnabled) {
				log.info("insertkey:" + key +",optime:" + op.getTimestamp() + "," + loginfobuff.toString());
			}
		}else if(op.getOperationType().isDelete()){
			String key = getPreparedStatementMapKey(op);
			PreparedStatement deleteState = get_delete_PreparedStatement(key,op);
			StringBuffer loginfobuff = new StringBuffer();
			for(Col column : op){
				if(!column.hasAfter() && !column.hasBefore()){
					continue;
				}
				Object obj = getAfterOrBeforeObject(column,false);
				
				deleteState.setObject(column.getIndex()+1, obj);
				loginfobuff.append(obj + "|");
			}
			deleteState.executeUpdate();
			if (isDebugEnabled) {
				log.info("deletekey:" + key +",optime:" + op.getTimestamp() + "," + loginfobuff.toString());
			}
		}else if(op.getOperationType().isUpdate()){
			String key = getPreparedStatementMapKey(op);
			if(key == null){
				return status;
			}
			PreparedStatement updateState = get_update_PreparedStatement(key, op);
			StringBuffer loginfobuff = new StringBuffer();
			int indx = 0;
			for(Col column : op){
				if(!column.hasAfter() && !column.hasBefore()){
					continue;
				}
				if(column.isChanged()){
					indx++;
					Object obj = getAfterOrBeforeObject(column,true);
					
					updateState.setObject(indx, obj);
					loginfobuff.append(obj + "|");
				}
			}
			for(Col column : op){
				if(!column.hasAfter() && !column.hasBefore()){
					continue;
				}
				if(!column.isChanged()){
					indx++;
					Object obj = getAfterOrBeforeObject(column,true);
					
					updateState.setObject(indx, obj);
				}
			}
			updateState.executeUpdate();
			if (isDebugEnabled) {
				log.info("updatekey:" + key +",optime:" + op.getTimestamp() + "," + loginfobuff.toString());
			}
		}

		return status;
	}

	@Override
	public void destroyHandler() throws SQLException {
		conn.close();
	}

	private String getPreparedStatementMapKey(Op op){
		if(op.getOperationType().isUpdate()){
			StringBuffer keypart = new StringBuffer();
			keypart.append(op.getTableName().getFullName() + "|");
			boolean hasUpdate = false;
			for(Col column : op) {
				if(!column.hasAfter() && !column.hasBefore()){
					continue;
				}
				if(column.isChanged()){
					hasUpdate = true;
				}else{
					keypart.append(column.getName() + "|");
				}
			}
			if(!hasUpdate){
				return null;
			}
			return keypart.substring(0, keypart.length()-1);
		}else{
			return op.getTableName().getFullName();
		}
	}
	
	private String getDstTableName(Op op){
		return (schemaName == null? op.getTableName().getSchemaName(): schemaName) + "." + op.getTableName().getShortName();
	}

	private PreparedStatement get_insert_PreparedStatement(String key,Op op) throws SQLException{
		if(!tableName_insertstat.containsKey(key)){
			StringBuffer prefix = new StringBuffer();
			prefix.append("insert into " + getDstTableName(op) + "(");
			StringBuffer postfix = new StringBuffer();
			for(Col column : op) {
				if(!column.hasAfter() && !column.hasBefore()){
					continue;
				}
				prefix.append(column.getName() + ",");
				postfix.append("?,");
			}
			String insertSql = prefix.substring(0, prefix.length()-1) + ") values(" + postfix.substring(0, postfix.length()-1) + ")";
			PreparedStatement state = conn.prepareStatement(insertSql);
			tableName_insertstat.put(key, state);
			if (isDebugEnabled) {
				log.info("insertKey:" + key + ",SQL:" + insertSql);
			}
		}
		return tableName_insertstat.get(key);
	}

	private PreparedStatement get_delete_PreparedStatement(String key,Op op) throws SQLException{
		if(!tableName_deletestat.containsKey(key)){
			StringBuffer prefix = new StringBuffer();
			prefix.append("delete from " + getDstTableName(op) + " where ");
			for(Col column : op) {
				if(!column.hasAfter() && !column.hasBefore()){
					continue;
				}
				prefix.append(column.getName() + "=? and ");
			}
			String deleteSql = prefix.substring(0, prefix.length()-4);
			PreparedStatement state = conn.prepareStatement(deleteSql);
			tableName_deletestat.put(key, state);
			if (isDebugEnabled) {
				log.info("deleteKey:" + key + ",SQL:" + deleteSql);
			}
		}
		return tableName_deletestat.get(key);
	}

	private PreparedStatement get_update_PreparedStatement(String key,Op op) throws SQLException{
		if(!tableName_updatestat.containsKey(key)){
			StringBuffer prefix = new StringBuffer();
			prefix.append("update " + getDstTableName(op) + " set ");
			StringBuffer postfix = new StringBuffer();
			for(Col column : op) {
				if(!column.hasAfter() && !column.hasBefore()){
					continue;
				}
				if(column.isChanged()){
					prefix.append(column.getName() + "=?,");
				}else{
					postfix.append(column.getName() + "=? and ");
				}
			}
			String updateSql = prefix.substring(0, prefix.length()-1) + " where " + postfix.substring(0, postfix.length()-4);
			PreparedStatement state = conn.prepareStatement(updateSql);
			tableName_updatestat.put(key, state);
			if (isDebugEnabled) {
				log.info("updateKey:" + key + ",SQL:" + updateSql);
			}
		}
		return tableName_updatestat.get(key);
	}

}
