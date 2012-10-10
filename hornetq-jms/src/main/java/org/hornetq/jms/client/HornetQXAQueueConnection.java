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

package org.hornetq.jms.client;

import javax.jms.ConnectionConsumer;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueSession;

import org.hornetq.api.core.client.ClientSessionFactory;

/**
 * HornetQ implementation of a JMS XAQueueConnection.
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class HornetQXAQueueConnection extends HornetQXAConnection implements XAQueueConnection
{
   public HornetQXAQueueConnection(final String username, final String password,
                                   final ClientSessionFactory sessionFactory,
                                   HornetQConnectionFactory hornetQConnectionFactory)
   {
      super(username, password, HornetQConnection.TYPE_QUEUE_CONNECTION, sessionFactory,
            hornetQConnectionFactory);
   }

   @Override
   public QueueSession createQueueSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      checkClosed();
      return (QueueSession)createSessionInternal(transacted, acknowledgeMode, HornetQSession.TYPE_QUEUE_SESSION);
   }

   @Override
   public ConnectionConsumer
            createConnectionConsumer(final Queue queue, final String messageSelector,
                                     final ServerSessionPool sessionPool, final int maxMessages) throws JMSException
   {
      checkClosed();
      checkTempQueues(queue);
      return null;
   }

   @Override
   public XAQueueSession createXAQueueSession() throws JMSException
   {
      checkClosed();
      return (XAQueueSession)createSessionInternal(true, Session.SESSION_TRANSACTED, HornetQSession.TYPE_QUEUE_SESSION);

   }
}
