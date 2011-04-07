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

package org.hornetq.jms.server.config.impl;

import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;

import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.JMSConfiguration;
import org.hornetq.jms.server.config.JMSQueueConfiguration;
import org.hornetq.jms.server.config.TopicConfiguration;


/**
 * A JMSConfigurationImpl
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class JMSConfigurationImpl implements JMSConfiguration
{

   private final List<ConnectionFactoryConfiguration> connectionFactoryConfigurations = new ArrayList<ConnectionFactoryConfiguration>();

   private final List<JMSQueueConfiguration> queueConfigurations = new ArrayList<JMSQueueConfiguration>();

   private final List<TopicConfiguration> topicConfigurations = new ArrayList<TopicConfiguration>();

   private final String domain;

   private Context context = null;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSConfigurationImpl()
   {
      domain = ConfigurationImpl.DEFAULT_JMX_DOMAIN;
   }

   public JMSConfigurationImpl(final List<ConnectionFactoryConfiguration> connectionFactoryConfigurations,
                               final List<JMSQueueConfiguration> queueConfigurations,
                               final List<TopicConfiguration> topicConfigurations,
                               final String domain)
   {
      this.connectionFactoryConfigurations.addAll(connectionFactoryConfigurations);
      this.queueConfigurations.addAll(queueConfigurations);
      this.topicConfigurations.addAll(topicConfigurations);
      this.domain = domain != null ? domain : ConfigurationImpl.DEFAULT_JMX_DOMAIN;
   }

   // JMSConfiguration implementation -------------------------------

   public List<ConnectionFactoryConfiguration> getConnectionFactoryConfigurations()
   {
      return connectionFactoryConfigurations;
   }

   public List<JMSQueueConfiguration> getQueueConfigurations()
   {
      return queueConfigurations;
   }

   public List<TopicConfiguration> getTopicConfigurations()
   {
      return topicConfigurations;
   }

   public Context getContext()
   {
      return context;
   }

   public void setContext(final Context context)
   {
      this.context = context;
   }

   public String getDomain()
   {
      return domain;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
