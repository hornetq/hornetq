/*
 * Copyright 2005-2014 Red Hat, Inc.
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

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicConnection;
import javax.jms.XATopicSession;

import org.hornetq.api.core.client.ClientSessionFactory;

/**
 * HornetQ implementation of a JMS XAConnection.
 * <p>
 * The flat implementation of {@link XATopicConnection} and {@link XAQueueConnection} is per design,
 * following common practices of JMS 1.1.
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public final class HornetQXAConnection extends HornetQConnection implements XATopicConnection, XAQueueConnection
{

   public HornetQXAConnection(final ConnectionFactoryOptions options, final String username, final String password, final int connectionType,
                              final String clientID, final int dupsOKBatchSize, final int transactionBatchSize,
                              final ClientSessionFactory sessionFactory)
   {
      super(options, username, password, connectionType, clientID, dupsOKBatchSize, transactionBatchSize, sessionFactory);
   }

   @Override
   public XASession createXASession() throws JMSException
   {
      checkClosed();
      return (XASession)createSessionInternal(isXA(), true, Session.SESSION_TRANSACTED, HornetQSession.TYPE_GENERIC_SESSION);
   }

   @Override
   public XAQueueSession createXAQueueSession() throws JMSException
   {
      checkClosed();
      return (XAQueueSession)createSessionInternal(isXA(), true, Session.SESSION_TRANSACTED, HornetQSession.TYPE_QUEUE_SESSION);

   }

   @Override
   public XATopicSession createXATopicSession() throws JMSException
   {
      checkClosed();
      return (XATopicSession)createSessionInternal(isXA(), true, Session.SESSION_TRANSACTED, HornetQSession.TYPE_TOPIC_SESSION);
   }

   @Override
   protected boolean isXA()
   {
      return true;
   }

}
