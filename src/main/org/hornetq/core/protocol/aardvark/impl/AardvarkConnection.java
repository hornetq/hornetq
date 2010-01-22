/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.protocol.aardvark.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Connection;

/**
 * A AardvarkConnection
 *
 * @author Tim Fox
 *
 *
 */
public class AardvarkConnection implements RemotingConnection
{
   private static final Logger log = Logger.getLogger(AardvarkConnection.class);

   private final AardvarkProtocolManager manager;
   
   private final Connection transportConnection;
      
   AardvarkConnection(final Connection transportConnection, final AardvarkProtocolManager manager)
   {
      this.transportConnection = transportConnection;
      
      this.manager = manager;
   }

   public void addCloseListener(CloseListener listener)
   {
   }

   public void addFailureListener(FailureListener listener)
   {
   }

   public boolean checkDataReceived()
   {
      return true;
   }

   public HornetQBuffer createBuffer(int size)
   {
      return HornetQBuffers.dynamicBuffer(size);
   }

   public void destroy()
   {
   }

   public void disconnect()
   {
   }

   public void fail(HornetQException me)
   {
   }

   public void flush()
   {  
   }

   public List<FailureListener> getFailureListeners()
   {
      return Collections.EMPTY_LIST;
   }

   public Object getID()
   {
      return transportConnection.getID();
   }

   public String getRemoteAddress()
   {      
      return transportConnection.getRemoteAddress();
   }

   public Connection getTransportConnection()
   {
      return transportConnection;
   }

   public boolean isClient()
   {
      return false;
   }

   public boolean isDestroyed()
   {
      return false;
   }

   public boolean removeCloseListener(CloseListener listener)
   {
      return false;
   }

   public boolean removeFailureListener(FailureListener listener)
   {
      return false;
   }

   public void setFailureListeners(List<FailureListener> listeners)
   {
   }

   public void bufferReceived(Object connectionID, HornetQBuffer buffer)
   {
      manager.handleBuffer(this, buffer);
   }
}
