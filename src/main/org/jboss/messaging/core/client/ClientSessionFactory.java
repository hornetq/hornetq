/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.core.client;

import java.util.Map;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;


/**
 * 
 * A ClientSessionFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientSessionFactory
{         
   ClientSession createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks,
                               int lazyAckBatchSize, boolean cacheProducers)
      throws MessagingException;
      
   ClientSession createSession(String username, String password, boolean xa, boolean autoCommitSends, boolean autoCommitAcks,
                               int lazyAckBatchSize, boolean cacheProducers)
      throws MessagingException;
        
   void setConsumerWindowSize(int size);
   
   int getConsumerWindowSize();
   
   void setProducerWindowSize(int size);     
   
   int getProducerWindowSize();
   
   void setConsumerMaxRate(int rate);
   
   int getConsumerMaxRate();
   
   void setProducerMaxRate(int rate);
   
   int getProducerMaxRate();
   
   boolean isBlockOnPersistentSend();
   
   void setBlockOnPersistentSend(final boolean blocking);
   
   boolean isBlockOnNonPersistentSend();
   
   void setBlockOnNonPersistentSend(final boolean blocking);
   
   boolean isBlockOnAcknowledge();
   
   void setBlockOnAcknowledge(final boolean blocking);
   
   ConnectorFactory getConnectorFactory();

   void setConnectorFactory(final ConnectorFactory connectorFactory);

   Map<String, Object> getTransportParams();

   void setTransportParams(final Map<String, Object> transportParams);

   long getPingPeriod();

   void setPingPeriod(final long pingPeriod);

   long getCallTimeout();

   void setCallTimeout(final long callTimeout);
   
}
