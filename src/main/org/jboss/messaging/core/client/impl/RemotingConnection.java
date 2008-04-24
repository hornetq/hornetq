package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;

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
   
   public long getSessionID();
 
   /**
    * Use this method if the packet is to be executed in the context of the targetID (i.e. for
    * sessions, connections & connections factories)
    */
   Packet send(long targetID, Packet packet) throws MessagingException;

   Packet send(long targetID, long executorID, Packet packet) throws MessagingException;
   
   Packet send(long targetID, long executorID, Packet packet, boolean oneWay) throws MessagingException;
   
   void setRemotingSessionListener(RemotingSessionListener newListener);
   
   PacketDispatcher getPacketDispatcher();
   
   public Location getLocation();
}
