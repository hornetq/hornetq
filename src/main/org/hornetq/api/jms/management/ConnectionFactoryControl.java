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

package org.hornetq.api.jms.management;

import java.util.List;

import org.hornetq.api.core.client.ClientSessionFactory;

/**
 * A ConnectionFactoryControl is used to manage a JMS ConnectionFactory.
 * <br>
 * HornetQ JMS ConnectionFactory uses an underlying ClientSessionFactory to connect to HornetQ servers.
 * Please refer to the ClientSessionFactory for a detailed description.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:fox@redhat.com">Tim Fox</a>
 * 
 * @see ClientSessionFactory
 */
public interface ConnectionFactoryControl
{
   /**
    * Returns the configuration name of this connection factory.
    */
   String getName();

   /**
    * Returns the JNDI bindings associated  to this connection factory.
    */
   List<String> getBindings();

   /**
    * Returns the Client ID of this connection factory (or {@code null} if it is not set.
    */
   String getClientID();

   /**
    * @see ClientSessionFactory#getClientFailureCheckPeriod()
    */
   long getClientFailureCheckPeriod();

   /**
    * @see ClientSessionFactory#getCallTimeout()
    */
   long getCallTimeout();

   /**
    * Returns the batch size (in bytes) between acknowledgements when using DUPS_OK_ACKNOWLEDGE mode.
    * 
    * @see ClientSessionFactory#getAckBatchSize()
    * @see javax.jms.Session#DUPS_OK_ACKNOWLEDGE
    */
   int getDupsOKBatchSize();

   /**
    * @see ClientSessionFactory#getConsumerMaxRate()
    */
   int getConsumerMaxRate();

   /**
    * @see ClientSessionFactory#getConsumerWindowSize()
    */
   int getConsumerWindowSize();

   /**
    * @see ClientSessionFactory#getProducerMaxRate()
    */
   int getProducerMaxRate();

   /**
    * @see ClientSessionFactory#getConfirmationWindowSize()
    */
   int getConfirmationWindowSize();

   /**
    * @see ClientSessionFactory#isBlockOnAcknowledge()
    */
   boolean isBlockOnAcknowledge();

   /**
    * @see ClientSessionFactory#isBlockOnDurableSend()
    */
   boolean isBlockOnDurableSend();

   /**
    * @see ClientSessionFactory#isBlockOnNonDurableSend()
    */
   boolean isBlockOnNonDurableSend();

   /**
    * @see ClientSessionFactory#isPreAcknowledge()
    */
   boolean isPreAcknowledge();

   /**
    * @see ClientSessionFactory#getConnectionTTL()
    */
   long getConnectionTTL();

   /**
    * Returns the batch size (in bytes) between acknowledgements when using a transacted session.
    * 
    * @see ClientSessionFactory#getAckBatchSize()
    */
   long getTransactionBatchSize();

   /**
    * @see ClientSessionFactory#getMinLargeMessageSize()
    */
   long getMinLargeMessageSize();

   /**
    * @see ClientSessionFactory#isAutoGroup()
    */
   boolean isAutoGroup();

   /**
    * @see ClientSessionFactory#getRetryInterval()
    */
   long getRetryInterval();

   /**
    * @see ClientSessionFactory#getRetryIntervalMultiplier()
    */
   double getRetryIntervalMultiplier();

   /**
    * @see ClientSessionFactory#getReconnectAttempts()
    */
   int getReconnectAttempts();

   /**
    * @see ClientSessionFactory#isFailoverOnServerShutdown()
    */
   boolean isFailoverOnServerShutdown();
}
