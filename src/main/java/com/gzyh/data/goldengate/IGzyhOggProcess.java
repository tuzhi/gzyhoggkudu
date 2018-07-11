package com.gzyh.data.goldengate;

import oracle.goldengate.datasource.DsConfiguration;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.adapt.Tx;
import oracle.goldengate.datasource.meta.DsMetaData;

public interface IGzyhOggProcess {
	public void initHandler(DsConfiguration conf, DsMetaData metaData) throws Exception;
	public Status process(Tx tx, Op op) throws Exception; 
	public void destroyHandler() throws Exception;
}
