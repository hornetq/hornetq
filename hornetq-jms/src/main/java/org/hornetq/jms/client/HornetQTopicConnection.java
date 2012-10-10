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
import javax.jms.ServerSessionPool;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.hornetq.api.core.client.ClientSessionFactory;

/**
 * HornetQ implementation of a JMS TopicConnection.
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class HornetQTopicConnection extends HornetQConnection implements TopicConnection
{
   public HornetQTopicConnection(final String username, final String password,
                                 final ClientSessionFactory sessionFactory,
                                 HornetQConnectionFactory hornetQConnectionFactory)
   {
      super(username, password, HornetQConnection.TYPE_TOPIC_CONNECTION, sessionFactory, hornetQConnectionFactory);
   }

   @Override
   public TopicSession createTopicSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      checkClosed();
      return (TopicSession)createSessionInternal(transacted, acknowledgeMode, false, HornetQSession.TYPE_TOPIC_SESSION);
   }

   @Override
   public ConnectionConsumer
            createConnectionConsumer(final Topic topic, final String messageSelector,
                                     final ServerSessionPool sessionPool, final int maxMessages) throws JMSException
   {
      checkClosed();
      checkTempQueues(topic);
      return null;
   }

}
