package com.gzyh.data.goldengate;

import oracle.goldengate.datasource.DsConfiguration;
import oracle.goldengate.datasource.DsTransaction;
import oracle.goldengate.datasource.adapt.Tx;
import oracle.goldengate.datasource.meta.DsMetaData;

public class TxFactory {
    public Tx createAdapterTx(DsTransaction transaction, DsMetaData metaData, DsConfiguration configuration) {
        return new Tx(transaction, metaData, configuration);
    }
}
