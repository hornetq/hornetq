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

import org.jboss.messaging.core.exception.MessagingException;


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
  
      
   /**
    * Set the default consumer window size value to use for consumers created from this connection factory.
    *     
    * @param the window size, measured in bytes.
    * A value of -1 signifies that consumer flow control is disabled.
    */
   void setDefaultConsumerWindowSize(int size);
   
   /**
    * Get the default consumer window size value to use for consumers created from this connection factory.
    * @param size
    * 
    * JBoss Messaging implements credit based consumer flow control, this value determines the initial pot of credits
    * the server has for the consumer. The server can only send messages to the consumer as long as it has
    * sufficient credits.
    * 
    * @return The default window size, measured in bytes.
    * A value of -1 signifies that consumer flow control is disabled.
    */
   int getDefaultConsumerWindowSize();
   
   /**
    * Set the default producer window size value to use for producers created from this connection factory.
    * 
    * @param the window size, measured in bytes.
    * A value of -1 signifies that producer flow control is disabled.
    */
   void setDefaultProducerWindowSize(int size);     
   
   /**
    * Get the default consumer window size value to use for consumers created from this connection factory.
    * 
    * JBoss Messaging implements credit based consumer flow control, this value determines the initial pot of credits
    * the server has for the consumer. The server can only send messages to the consumer as long as it has
    * sufficient credits.
    * 
    * @return The default window size, measured in bytes.
    * A value of -1 signifies that consumer flow control is disabled.
    */
   int getDefaultProducerWindowSize();
   
   /**
    * Set the default consumer maximum consume rate for consumers created from this connection factory.
    * @param rate- the maximum consume rate, measured in messages / second
    * A value of -1 signifies there is no maximum rate limit
    */
   void setDefaultConsumerMaxRate(int rate);
   
   /**
    * Get the default consumer maximum consume rate for consumers created from this connection factory.
    * @return the maximum consume rate, measured in messages / second
    * A value of -1 signifies there is no maximum rate limit
    */
   int getDefaultConsumerMaxRate();
   
   /**
    * Set the default producer maximum send rate for producers created from this connection factory.
    * @param rate- the maximum send rate, measured in messages / second
    * A value of -1 signifies there is no maximum rate limit
    */
   void setDefaultProducerMaxRate(int rate);
   
   /**
    * Get the default producer maximum send rate for producers created from this connection factory.
    * @return the maximum send rate, measured in messages / second
    * A value of -1 signifies there is no maximum rate limit
    */
   int getDefaultProducerMaxRate();
   
   /**
    * Get the default value of whether producers created from this connection factory will send persistent messages
    * blocking.
    * @return Whether persistent messages are sent blocking
    */
   boolean isDefaultBlockOnPersistentSend();
   
   /**
    * Set the default value of whether producers created from this connection factory will send persistent messages
    * blocking.
    * @param blocking Whether persistent messages are sent blocking
    */
   void setDefaultBlockOnPersistentSend(final boolean blocking);
   
   /**
    * Get the default value of whether producers created from this connection factory will send non persistent messages
    * blocking.
    * @return Whether non persistent messages are sent blocking
    */
   boolean isDefaultBlockOnNonPersistentSend();
   
   /**
    * Set the default value of whether producers created from this connection factory will send non persistent messages
    * blocking.
    * @param blocking Whether non persistent messages are sent blocking
    */
   void setDefaultBlockOnNonPersistentSend(final boolean blocking);
   
   /**
    * Get the default value of whether producers created from this connection factory will send acknowledgements
    * blocking
    * @return Whether acknowledgements are sent blocking
    */
   boolean isDefaultBlockOnAcknowledge();
   
   /**
    * Set the default value of whether producers created from this connection factory will send acknowledgements
    * blockiong
    * @param blocking Whether acknowledgements are sent blocking
    */
   void setDefaultBlockOnAcknowledge(final boolean blocking);
   
   /**
    * Get the location of the server for this connection factory
    * @return The location
    */
   Location getLocation();
   
   /**
    * Get the connection params used when creating connections using this connection factory
    */
   ConnectionParams getConnectionParams();
   
   /**
    * Set the connection params to be used when creating connections using this connection factory
    * @param params
    */
   void setConnectionParams(ConnectionParams connectionParams);
}
