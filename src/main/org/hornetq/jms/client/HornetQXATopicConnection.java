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

import javax.jms.JMSException;
import javax.jms.TopicSession;
import javax.jms.XATopicConnection;

import org.hornetq.api.core.client.ClientSessionFactory;

/**
 * HornetQ implementation of a JMS XATopicConnection.
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class HornetQXATopicConnection extends HornetQConnection implements XATopicConnection
{
   public HornetQXATopicConnection(final String username,
                                   final String password,
                                   final int connectionType,
                                   final String clientID,
                                   final int dupsOKBatchSize,
                                   final int transactionBatchSize,
                                   final ClientSessionFactory sessionFactory)
   {
      super(username, password, connectionType, clientID, dupsOKBatchSize, transactionBatchSize, sessionFactory);
   }

   public TopicSession createTopicSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      checkClosed();
      return createSessionInternal(transacted, acknowledgeMode, true, HornetQSession.TYPE_TOPIC_SESSION);
   }
}
