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

import org.jboss.messaging.core.management.AcceptorControl;
import org.jboss.messaging.core.management.AddressControl;
import org.jboss.messaging.core.management.BridgeControl;
import org.jboss.messaging.core.management.BroadcastGroupControl;
import org.jboss.messaging.core.management.ClusterConnectionControl;
import org.jboss.messaging.core.management.DiscoveryGroupControl;
import org.jboss.messaging.core.management.DivertControl;
import org.jboss.messaging.core.management.MessagingServerControl;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.management.QueueControl;
import org.jboss.messaging.jms.server.management.ConnectionFactoryControl;
import org.jboss.messaging.jms.server.management.JMSQueueControl;
import org.jboss.messaging.jms.server.management.JMSServerControl;
import org.jboss.messaging.jms.server.management.TopicControl;
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

   public static AcceptorControl createAcceptorControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (AcceptorControl)createProxy(ObjectNames.getAcceptorObjectName(name),
                                               AcceptorControl.class,
                                               mbeanServer);
   }

   public static BroadcastGroupControl createBroadcastGroupControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (BroadcastGroupControl)createProxy(ObjectNames.getBroadcastGroupObjectName(name),
                                                     BroadcastGroupControl.class,
                                                     mbeanServer);
   }

   public static DiscoveryGroupControl createDiscoveryGroupControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (DiscoveryGroupControl)createProxy(ObjectNames.getDiscoveryGroupObjectName(name),
                                                     DiscoveryGroupControl.class,
                                                     mbeanServer);
   }

   public static BridgeControl createBridgeControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (BridgeControl)createProxy(ObjectNames.getBridgeObjectName(name),
                                                  BridgeControl.class,
                                                  mbeanServer);
   }

   public static DivertControl createDivertControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (DivertControl)createProxy(ObjectNames.getDivertObjectName(new SimpleString(name)),
                                             DivertControl.class,
                                             mbeanServer);
   }

   public static ClusterConnectionControl createClusterConnectionControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (ClusterConnectionControl)createProxy(ObjectNames.getClusterConnectionObjectName(name),
                                                        ClusterConnectionControl.class,
                                                        mbeanServer);
   }

   public static MessagingServerControl createMessagingServerControl(MBeanServer mbeanServer) throws Exception
   {
      return (MessagingServerControl)createProxy(ObjectNames.getMessagingServerObjectName(),
                                                      MessagingServerControl.class,
                                                      mbeanServer);
   }

   public static QueueControl createQueueControl(SimpleString address, SimpleString name, MBeanServer mbeanServer) throws Exception
   {
      return (QueueControl)createProxy(ObjectNames.getQueueObjectName(address, name),
                                            QueueControl.class,
                                            mbeanServer);
   }

   public static AddressControl createAddressControl(SimpleString address, MBeanServer mbeanServer) throws Exception
   {
      return (AddressControl)createProxy(ObjectNames.getAddressObjectName(address),
                                              AddressControl.class,
                                              mbeanServer);
   }

   public static JMSQueueControl createJMSQueueControl(Queue queue, MBeanServer mbeanServer) throws Exception
   {
      return createJMSQueueControl(queue.getQueueName(), mbeanServer);
   }

   public static JMSQueueControl createJMSQueueControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (JMSQueueControl)createProxy(ObjectNames.getJMSQueueObjectName(name),
                                               JMSQueueControl.class,
                                               mbeanServer);
   }

   public static JMSServerControl createJMSServerControl(MBeanServer mbeanServer) throws Exception
   {
      return (JMSServerControl)createProxy(ObjectNames.getJMSServerObjectName(),
                                                JMSServerControl.class,
                                                mbeanServer);
   }

   public static ConnectionFactoryControl createConnectionFactoryControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (ConnectionFactoryControl)createProxy(ObjectNames.getConnectionFactoryObjectName(name),
                                                        ConnectionFactoryControl.class,
                                                        mbeanServer);
   }

   public static TopicControl createTopicControl(Topic topic, MBeanServer mbeanServer) throws Exception
   {
      return (TopicControl)createProxy(ObjectNames.getJMSTopicObjectName(topic.getTopicName()),
                                            TopicControl.class,
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
