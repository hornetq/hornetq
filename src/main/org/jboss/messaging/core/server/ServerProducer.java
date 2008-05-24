package org.jboss.messaging.core.server;


/**
 * 
 * A ServerProducer
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ServerProducer
{
	long getID();
	
	void close() throws Exception;
	
	void send(ServerMessage msg) throws Exception;
	
	void requestAndSendCredits() throws Exception;
	
	void sendCredits(int credits) throws Exception;
	
	void setWaiting(boolean waiting);
	
	boolean isWaiting();
}
