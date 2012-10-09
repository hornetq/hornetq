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

package org.hornetq.jms.server;

import java.io.InputStream;

import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.JMSConfiguration;
import org.hornetq.jms.server.config.JMSQueueConfiguration;
import org.hornetq.jms.server.config.TopicConfiguration;
import org.w3c.dom.Node;

/**
 * A JMSServerConfigParser
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public interface JMSServerConfigParser
{
   /**
    * Parse the JMS Configuration XML as a JMSConfiguration object
    */
   JMSConfiguration parseConfiguration(final InputStream stream) throws Exception;

   /**
    * Parse the JMS Configuration XML as a JMSConfiguration object
    */
   JMSConfiguration parseConfiguration(final Node rootnode) throws Exception;

   /**
    * Parse the topic node as a TopicConfiguration object
    * @param node
    * @return {@link TopicConfiguration} parsed from the node
    * @throws Exception
    */
   TopicConfiguration parseTopicConfiguration(final Node node) throws Exception;

   /**
    * Parse the Queue Configuration node as a QueueConfiguration object
    * @param node
    * @return {@link JMSQueueConfiguration} parsed from the node
    * @throws Exception
    */
   JMSQueueConfiguration parseQueueConfiguration(final Node node) throws Exception;

   /**
    * Parse the Connection Configuration node as a ConnectionFactoryConfiguration object
    * @param node
    * @return
    * @throws Exception
    */
   ConnectionFactoryConfiguration parseConnectionFactoryConfiguration(final Node node) throws Exception;
}
