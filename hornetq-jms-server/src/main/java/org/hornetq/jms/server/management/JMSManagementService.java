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

package org.hornetq.jms.server.management;

import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.core.server.Queue;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQQueue;
import org.hornetq.jms.client.HornetQTopic;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public interface JMSManagementService
{
   JMSServerControl registerJMSServer(JMSServerManager server) throws Exception;

   void unregisterJMSServer() throws Exception;

   void registerQueue(HornetQQueue queue, Queue serverQueue) throws Exception;

   void unregisterQueue(String name) throws Exception;

   void registerTopic(HornetQTopic topic) throws Exception;

   void unregisterTopic(String name) throws Exception;

   void registerConnectionFactory(String name, ConnectionFactoryConfiguration config, HornetQConnectionFactory connectionFactory) throws Exception;

   void unregisterConnectionFactory(String name) throws Exception;

   void stop() throws Exception;
}
