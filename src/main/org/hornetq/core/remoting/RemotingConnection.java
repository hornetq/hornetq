/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.remoting;

import java.util.List;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.spi.BufferHandler;
import org.hornetq.core.remoting.spi.Connection;

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

   Channel getChannel(long channelID, int confWindowSize);
   
   void putChannel(long channelID, Channel channel);
   
   boolean removeChannel(long channelID);

   long generateChannelID();

   void addFailureListener(FailureListener listener);

   boolean removeFailureListener(FailureListener listener);
   
   void addCloseListener(CloseListener listener);
      
   boolean removeCloseListener(CloseListener listener);

   List<FailureListener> getFailureListeners();

   void setFailureListeners(List<FailureListener> listeners);

   HornetQBuffer createBuffer(int size);

   void fail(HornetQException me);

   void destroy();

   void syncIDGeneratorSequence(long id);

   long getIDGeneratorSequence();

   Connection getTransportConnection();
   
   boolean isClient();
   
   boolean isDestroyed();
   
   long getBlockingCallTimeout();
   
   Object getTransferLock(); 
   
   boolean checkDataReceived();
}
