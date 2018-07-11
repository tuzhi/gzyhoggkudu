package com.gzyh.data.goldengate;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import oracle.goldengate.datasource.AbstractHandler;
import oracle.goldengate.datasource.DsConfiguration;
import oracle.goldengate.datasource.DsEvent;
import oracle.goldengate.datasource.DsOperation;
import oracle.goldengate.datasource.DsTransaction;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.TxOpMode;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.adapt.Tx;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;

public class BaseEventHandler extends AbstractHandler {
	private static final Logger log = LoggerFactory.getLogger(BaseEventHandler.class);
	//外部公共参数
	protected boolean isMetricsEnabled = false;
	protected boolean isDebugEnabled = false;
	protected String metricsLoggerName = "goldengate.gzyh.metrics";
	protected Long metricsReportFrequencyInSeconds = (long) 360;
	protected String gzyhOggProcessType = "kudu";
	protected String schemaName = null;
	protected boolean primary_update = false;
	protected boolean from_oracle = true;
	//数据存储接口
	protected IGzyhOggProcess ggprocessor = null;
	//统计变量
	private AtomicLong numOps = new AtomicLong(0);
	private AtomicLong numTxs = new AtomicLong(0);
	private MetricRegistry metrics = new MetricRegistry();
	private Timer operationProcessingTimer = metrics.timer("operationProcessingTime");
	private Meter operationProcessingErrorMeter = metrics.meter("processingErrors");
	private ScheduledReporter metricsReporter;
	private TxFactory txFactory;

	public void setIsMetricsEnabled(boolean isMetricsEnabled) {
		this.isMetricsEnabled = isMetricsEnabled;
	}

	public void setIsDebugEnabled(boolean isDebugEnabled) {
		this.isDebugEnabled = isDebugEnabled;
	}

	public void setMetricsLoggerName(String metricsLoggerName) {
		this.metricsLoggerName = metricsLoggerName;
	}

	public void setMetricsReportFrequencyInSeconds(
			Long metricsReportFrequencyInSeconds) {
		this.metricsReportFrequencyInSeconds = metricsReportFrequencyInSeconds;
	}

	public void setGzyhOggProcessType(String gzyhOggProcessType) {
		this.gzyhOggProcessType = gzyhOggProcessType;
	}
	
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public void setPrimary_update(boolean primary_update) {
		this.primary_update = primary_update;
	}
	
	public void setFrom_oracle(boolean from_oracle) {
		this.from_oracle = from_oracle;
	}

	public BaseEventHandler() {
		super(TxOpMode.op);
		log.info("created handler - default mode: " + getMode());
	}

	@Override
	public void init(DsConfiguration conf, DsMetaData metaData) {
		super.init(conf, metaData);
		log.info("Initializing handler: Mode =" + getMode());
		txFactory = new TxFactory();

		if (isMetricsEnabled) {
			if(metricsReporter == null){
				metricsReporter = createReporter(metricsLoggerName,metricsReportFrequencyInSeconds,metrics);
			}
		}
		try {
			ggprocessor = (IGzyhOggProcess) this;
			ggprocessor.initHandler(conf, metaData);
		} catch(ClassNotFoundException ce){
			log.error("Initializing IGoldenGateProcess goldenGateProcessType not found:" + gzyhOggProcessType);
		} catch (Exception e) {
			log.error("Initializing IGoldenGateProcess:" + ggprocessor.getClass().getName() + " error:"+  e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public Status transactionBegin(DsEvent e, DsTransaction transaction) {
		super.transactionBegin(e, transaction);

		if (isDebugEnabled) {
			log.info("Received begin tx event, numTx="
					+ numTxs.get()
					+ " : position="
					+ transaction.getTranID()
					+ ", totalOps="
					+ transaction.getTotalOps());
		}

		return Status.OK;
	}

	@Override
	public Status operationAdded(DsEvent e, DsTransaction transaction, DsOperation operation) {
		Status overallStatus = Status.OK;
		super.operationAdded(e, transaction, operation);
		numOps.incrementAndGet();

		final Tx tx = new Tx(transaction, getMetaData(), getConfig());
		final TableMetaData tMeta = getMetaData().getTableMetaData(operation.getTableName());
		final Op op = new Op(operation, tMeta, getConfig());

		operation.getTokens();
		if (isOperationMode()) {

			if (isDebugEnabled) {
				log.info(" Received operation: table='"
						+ op.getTableName() + "'"
						+ ", pos=" + op.getPosition()
						+ " (total_ops= " + tx.getTotalOps()
						+ ", buffered=" + tx.getSize() + ")"
						+ ", ts=" + op.getTimestamp());

			}

			Status operationStatus = processOperation(tx, op);

			if (Status.ABEND.equals(operationStatus)) {
				overallStatus = Status.ABEND;
			}
		}
		return overallStatus;
	}


	@Override
	public Status transactionCommit(DsEvent e, DsTransaction transaction) {
		Status overallStatus = Status.OK;
		super.transactionCommit(e, transaction);

		Tx tx = txFactory.createAdapterTx(transaction, getMetaData(), getConfig());
		numTxs.incrementAndGet();

		if (!isOperationMode()) {
			for (Op op : tx) {
				Status operationStatus = processOperation(tx, op);

				if (Status.ABEND.equals(operationStatus)) {
					overallStatus = Status.ABEND;
				}
			}
		}

		if (isDebugEnabled) {
			log.info("transactionCommit event, tx #" + numTxs.get() + ":"
					+ ", pos=" + tx.getTranID()
					+ " (total_ops= " + tx.getTotalOps()
					+ ", buffered=" + tx.getSize() + ")"
					+ ", ts=" + tx.getTimestamp() + ")");
		}

		return overallStatus;
	}

	private Status processOperation(Tx tx, Op op) {
		Timer.Context timer = operationProcessingTimer.time();
		Status status = Status.OK;
		try {
			status = ggprocessor.process(tx, op);
		} catch (Exception re) {
			operationProcessingErrorMeter.mark();
			log.error("Error processing operation: " + op.toString(), re);
			status = Status.ABEND;
		}

		timer.stop();
		return status;
	}

	@Override
	public Status metaDataChanged(DsEvent e, DsMetaData meta) {
		log.debug("Received metadata event: " + e 
				+ ";current tables: " 
				+ meta.getTableNames().size());
		return super.metaDataChanged(e, meta);
	}

	@Override
	public String reportStatus() {
		String s = "Status report: " 
				+ ", mode=" + getMode() 
				+ ", transactions=" + ", operations=";
		return s;
	}

	@Override
	public void destroy() {
		log.debug("destroy()... " + reportStatus());
		if (isMetricsEnabled) {
			metricsReporter.stop();
		}

		try {
			ggprocessor.destroyHandler();
		} catch (Exception e) {
			log.error("destroy() error:" + e.getMessage());
			e.printStackTrace();
		}
		super.destroy();
	}

	public static ScheduledReporter createReporter(String metricsLoggerName,Long metricsReportFrequencyInSeconds,MetricRegistry metrics) {
		ScheduledReporter reporter = Slf4jReporter.forRegistry(metrics)
				.outputTo(LoggerFactory.getLogger(metricsLoggerName))
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS)
				.build();

		reporter.start(metricsReportFrequencyInSeconds, TimeUnit.SECONDS);

		return reporter;
	}
}