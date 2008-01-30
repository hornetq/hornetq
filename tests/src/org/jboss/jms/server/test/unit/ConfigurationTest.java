/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.server.test.unit;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashSet;

import junit.framework.TestCase;

import org.jboss.jms.server.security.Role;
import org.jboss.messaging.core.Configuration;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.TransportType;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConfigurationTest extends TestCase
{
   private Configuration configuration;

   protected void setUp() throws Exception
   {
      configuration = new Configuration();
      configuration.setConfigurationUrl("ConfigurationTest-config.xml");
      configuration.start();
   }


   protected void tearDown() throws Exception
   {
      configuration = null;
   }

   public void testSetServerPeerId() throws Exception
   {
      assertEquals("failed to set ServerPeerId", new Integer(10), configuration.getMessagingServerID());
   }

   public void testSetDefaultQueueJndiContext() throws Exception
   {
      assertEquals("failed to set default queue jndi context", "/queuetest", configuration.getDefaultQueueJNDIContext());
   }

   public void testSetDefaultTopicJndiContext() throws Exception
   {
      assertEquals("failed to set default topic jndi context", "/topictest", configuration.getDefaultTopicJNDIContext());
   }

   public void testSetSecurityDomain() throws Exception
   {
      assertEquals("failed to set security domain", "java:/jaas/messagingtest", configuration.getSecurityDomain());
   }

   public void testSetDefaultSecurity() throws Exception
   {
      assertEquals("not enough entries should be 4", 4, configuration.getSecurityConfig().size());
      HashSet<Role> roles = configuration.getSecurityConfig();
      for (Role role : roles)
      {
         if ("guest1".equals(role.getName()))
         {
            assertEquals("read configured incorrectly", true, role.isRead());
            assertEquals("write configured incorrectl", true, role.isWrite());
            assertEquals("create configured incorrectl", true, role.isCreate());
         }
         else if ("guest2".equals(role.getName()))
         {
            assertEquals("read configured incorrectly", true, role.isRead());
            assertEquals("write configured incorrectl", true, role.isWrite());
            assertEquals("create configured incorrectl", false, role.isCreate());
         }
         else if ("guest3".equals(role.getName()))
         {
            assertEquals("read configured incorrectly", true, role.isRead());
            assertEquals("write configured incorrectl", false, role.isWrite());
            assertEquals("create configured incorrectl", false, role.isCreate());
         }
         else if ("guest4".equals(role.getName()))
         {
            assertEquals("read configured incorrectly", false, role.isRead());
            assertEquals("write configured incorrectl", false, role.isWrite());
            assertEquals("create configured incorrectl", false, role.isCreate());
         }
         else
         {
            fail("not all roles names configured properly:" + role);
         }
      }
   }

   public void testSetDLQ() throws Exception
   {
      assertEquals("failed to set DLQ", "DLQtest", configuration.getDefaultDLQ());
   }

   public void testSetDefMaxDelAtt() throws Exception
   {
      assertEquals("failed to set Default max delivery attempts", new Integer(100), configuration.getDefaultMaxDeliveryAttempts());
   }

   public void testSetExpiryQueue() throws Exception
   {
      assertEquals("failed to set DLQ", "ExpiryQueuetest", configuration.getDefaultExpiryQueue());
   }

   public void testSetDefaultRedeliveryDelay() throws Exception
   {
      assertEquals("failed to set Default redeliver delay", 10, configuration.getDefaultRedeliveryDelay());
   }

   public void testSetMessageCounterSamplePeriod() throws Exception
   {
      assertEquals("failed to set Message Counter sample period", 50000, configuration.getMessageCounterSamplePeriod());
   }

   public void testSetDefaultMessageCounterHistory() throws Exception
   {
      assertEquals("failed to set default message counter history", new Integer(21), configuration.getDefaultMessageCounterHistoryDayLimit());
   }

   public void testSetStrictTck() throws Exception
   {
      assertEquals("failed to set strict tck", Boolean.TRUE, configuration.isStrictTck());
   }

   public void testSetClustered() throws Exception
   {
      assertEquals("failed to set clustered", Boolean.TRUE, configuration.isClustered());
   }

   public void testSetGroupName() throws Exception
   {
      assertEquals("failed to set group name", "MessagingPostOfficetest", configuration.getGroupName());
   }

   public void testSetStateTimeout() throws Exception
   {
      assertEquals("failed to set state timeout", new Long(10000), configuration.getStateTimeout());
   }

   public void testSetCastTimeout() throws Exception
   {
      assertEquals("failed to set cast timeout", new Long(10000), configuration.getCastTimeout());
   }

   public void testSetControlChannelName() throws Exception
   {
      assertEquals("failed to set control channel name", "udp-synctest", configuration.getControlChannelName());
   }

   public void testSetDateChannelName() throws Exception
   {
      assertEquals("failed to set data channel name", "udptest", configuration.getDataChannelName());
   }

   public void testSetChannelPartitionName() throws Exception
   {
      assertEquals("failed to set channel partition name", "JMStest", configuration.getChannelPartitionName());
   }

   public void testSetRemotingConfiguration() throws Exception
   {
      RemotingConfiguration remotingConfig = configuration.getRemotingConfiguration();
      assertEquals(TransportType.TCP, remotingConfig.getTransport());
      assertEquals(10000, remotingConfig.getPort());
      assertEquals(100, remotingConfig.getTimeout());
   }

   public void testPropertyChangeListener() throws Exception
   {
      MyListener listener = new MyListener();
      configuration.addPropertyChangeListener(listener);
      configuration.setSecurityConfig(null);
      assertTrue("property change listener not fired", listener.isCalled());
      listener.setCalled(false);
      configuration.setMessageCounterSamplePeriod(1000000);
      assertTrue("property change listener not fired", listener.isCalled());
   }
   class MyListener implements PropertyChangeListener
   {
      boolean called = false;

      public boolean isCalled()
      {
         return called;
      }

      public void setCalled(boolean called)
      {
         this.called = called;
      }

      public void propertyChange(PropertyChangeEvent evt)
      {
         called = true;
      }
   }
}
