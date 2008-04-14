package org.jboss.messaging.core.server;

import org.jboss.messaging.core.message.Message;

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
	
	void send(String address, Message msg) throws Exception;
	
	void sendCredits() throws Exception;
	
	void setWaiting(boolean waiting);
	
	boolean isWaiting();
}
