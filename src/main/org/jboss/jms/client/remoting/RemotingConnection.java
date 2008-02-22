package org.jboss.jms.client.remoting;

import org.jboss.jms.client.api.FailureListener;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.util.MessagingException;

/**
 * 
 * A RemotingConnection
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface RemotingConnection
{
	public void start() throws Throwable;

   public void stop();
   
   public String getSessionID();
 
   AbstractPacket send(String id, AbstractPacket packet) throws MessagingException;
   
   AbstractPacket send(String id, AbstractPacket packet, boolean oneWay) throws MessagingException;
   
   void setFailureListener(FailureListener newListener);
}
