/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.hornetq.core.remoting;

import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.remoting.spi.BufferHandler;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.MessagingBuffer;

import java.util.List;

/**
 * A RemotingConnection
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface RemotingConnection extends BufferHandler
{
   Object getID();

   String getRemoteAddress();

   Channel getChannel(long channelID, int windowSize, boolean block);
   
   void putChannel(long channelID, Channel channel);
   
   boolean removeChannel(long channelID);

   long generateChannelID();

   void addFailureListener(FailureListener listener);

   boolean removeFailureListener(FailureListener listener);
   
   void addCloseListener(CloseListener listener);
      
   boolean removeCloseListener(CloseListener listener);

   List<FailureListener> getFailureListeners();

   void setFailureListeners(List<FailureListener> listeners);

   MessagingBuffer createBuffer(int size);

   void fail(MessagingException me);

   void destroy();

   void syncIDGeneratorSequence(long id);

   long getIDGeneratorSequence();

   void activate();

   void freeze();
  
   Connection getTransportConnection();
   
   boolean isActive();
   
   boolean isClient();
   
   boolean isDestroyed();
   
   long getBlockingCallTimeout();
   
   Object getTransferLock(); 
   
   boolean checkDataReceived();
}
