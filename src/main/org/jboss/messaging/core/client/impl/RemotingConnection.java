package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.MessagingException;
import org.jboss.messaging.core.client.FailureListener;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;

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
