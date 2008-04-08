package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.FailureListener;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;

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
 
   /**
    * Use this method if the packet is to be executed in the context of the targetID (i.e. for
    * sessions, connections & connections factories)
    */
   Packet send(String targetID, Packet packet) throws MessagingException;

   Packet send(String targetID, String executorID, Packet packet) throws MessagingException;
   
   Packet send(String targetID, String executorID, Packet packet, boolean oneWay) throws MessagingException;
   
   void setFailureListener(FailureListener newListener);
   
   PacketDispatcher getPacketDispatcher();
}
