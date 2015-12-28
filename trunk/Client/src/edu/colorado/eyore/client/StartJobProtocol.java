package edu.colorado.eyore.client;


import java.util.logging.Logger;


import edu.colorado.eyore.common.job.JobDescriptor;
import edu.colorado.eyore.common.net.MessageObjectUtil;
import edu.colorado.eyore.common.net.Protocol;

public class StartJobProtocol implements Protocol {

	Logger logger = Logger.getLogger(StartJobProtocol.class.getName());
	
	protected boolean isFinished;
	protected boolean jobWasStarted = false;
	protected JobDescriptor jDesc;
	
	public StartJobProtocol(JobDescriptor jDesc){
		isFinished = false;
		this.jDesc = jDesc;
	}
	
	@Override
	public Boolean isFinished() {
		return isFinished;
	}

	
	@Override
	public String respondTo(String rcvFromFarEnd) {
		// start by requesting that the server starts the job
		if(rcvFromFarEnd == null){
			return "CLIENT REQUEST JOB START:" + 
				MessageObjectUtil.objectToString(jDesc);
		}
		
		// server responds with OK to request job
		// and info for client to write stuff to HDFS
		// followed by client telling server OK to start
		// the job
		if(rcvFromFarEnd.startsWith("JOB STARTED:")){
			isFinished = true;
			jobWasStarted = true;
			return null;
		}else{
			logger.severe("JOB FAILED TO START" + rcvFromFarEnd);
			jobWasStarted = false;
			isFinished = true;
			return null;
		}
	}

	@Override
	public Object getData() {
		return jobWasStarted;
	}

}
