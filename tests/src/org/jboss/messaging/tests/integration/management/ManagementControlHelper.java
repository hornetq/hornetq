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

package org.jboss.messaging.tests.integration.management;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.jboss.messaging.core.management.AcceptorControlMBean;
import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.BridgeControlMBean;
import org.jboss.messaging.core.management.BroadcastGroupControlMBean;
import org.jboss.messaging.core.management.ClusterConnectionControlMBean;
import org.jboss.messaging.core.management.DiscoveryGroupControlMBean;
import org.jboss.messaging.core.management.DivertControlMBean;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.jms.server.management.ConnectionFactoryControlMBean;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.utils.SimpleString;

/**
 * A ManagementControlHelper
 *
 * @author jmesnil
 *
 */
public class ManagementControlHelper
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static AcceptorControlMBean createAcceptorControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (AcceptorControlMBean)createProxy(ObjectNames.getAcceptorObjectName(name),
                                               AcceptorControlMBean.class,
                                               mbeanServer);
   }

   public static BroadcastGroupControlMBean createBroadcastGroupControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (BroadcastGroupControlMBean)createProxy(ObjectNames.getBroadcastGroupObjectName(name),
                                                     BroadcastGroupControlMBean.class,
                                                     mbeanServer);
   }

   public static DiscoveryGroupControlMBean createDiscoveryGroupControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (DiscoveryGroupControlMBean)createProxy(ObjectNames.getDiscoveryGroupObjectName(name),
                                                     DiscoveryGroupControlMBean.class,
                                                     mbeanServer);
   }

   public static BridgeControlMBean createBridgeControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (BridgeControlMBean)createProxy(ObjectNames.getBridgeObjectName(name),
                                                  BridgeControlMBean.class,
                                                  mbeanServer);
   }

   public static DivertControlMBean createDivertControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (DivertControlMBean)createProxy(ObjectNames.getDivertObjectName(new SimpleString(name)),
                                             DivertControlMBean.class,
                                             mbeanServer);
   }

   public static ClusterConnectionControlMBean createClusterConnectionControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (ClusterConnectionControlMBean)createProxy(ObjectNames.getClusterConnectionObjectName(name),
                                                        ClusterConnectionControlMBean.class,
                                                        mbeanServer);
   }

   public static MessagingServerControlMBean createMessagingServerControl(MBeanServer mbeanServer) throws Exception
   {
      return (MessagingServerControlMBean)createProxy(ObjectNames.getMessagingServerObjectName(),
                                                      MessagingServerControlMBean.class,
                                                      mbeanServer);
   }

   public static QueueControlMBean createQueueControl(SimpleString address, SimpleString name, MBeanServer mbeanServer) throws Exception
   {
      return (QueueControlMBean)createProxy(ObjectNames.getQueueObjectName(address, name),
                                            QueueControlMBean.class,
                                            mbeanServer);
   }

   public static AddressControlMBean createAddressControl(SimpleString address, MBeanServer mbeanServer) throws Exception
   {
      return (AddressControlMBean)createProxy(ObjectNames.getAddressObjectName(address),
                                              AddressControlMBean.class,
                                              mbeanServer);
   }

   public static JMSQueueControlMBean createJMSQueueControl(Queue queue, MBeanServer mbeanServer) throws Exception
   {
      return createJMSQueueControl(queue.getQueueName(), mbeanServer);
   }

   public static JMSQueueControlMBean createJMSQueueControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (JMSQueueControlMBean)createProxy(ObjectNames.getJMSQueueObjectName(name),
                                               JMSQueueControlMBean.class,
                                               mbeanServer);
   }

   public static JMSServerControlMBean createJMSServerControl(MBeanServer mbeanServer) throws Exception
   {
      return (JMSServerControlMBean)createProxy(ObjectNames.getJMSServerObjectName(),
                                                JMSServerControlMBean.class,
                                                mbeanServer);
   }

   public static ConnectionFactoryControlMBean createConnectionFactoryControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (ConnectionFactoryControlMBean)createProxy(ObjectNames.getConnectionFactoryObjectName(name),
                                                        ConnectionFactoryControlMBean.class,
                                                        mbeanServer);
   }

   public static TopicControlMBean createTopicControl(Topic topic, MBeanServer mbeanServer) throws Exception
   {
      return (TopicControlMBean)createProxy(ObjectNames.getJMSTopicObjectName(topic.getTopicName()),
                                            TopicControlMBean.class,
                                            mbeanServer);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static Object createProxy(ObjectName objectName, Class mbeanInterface, MBeanServer mbeanServer)
   {
      return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName, mbeanInterface, false);
   }

   // Inner classes -------------------------------------------------

}
