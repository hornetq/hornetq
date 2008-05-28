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
package org.jboss.messaging.tests.unit.core.config.impl;

import junit.framework.TestCase;
import org.jboss.messaging.core.config.impl.FileConfiguration;
import org.jboss.messaging.core.remoting.TransportType;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConfigurationTest extends TestCase
{
   private FileConfiguration configuration;

   protected void setUp() throws Exception
   {
      configuration = new FileConfiguration();
      configuration.setConfigurationUrl("ConfigurationTest-config.xml");
      configuration.start();
   }

   protected void tearDown() throws Exception
   {
      configuration = null;
   }

   public void testSetClustered() throws Exception
   {
      assertEquals("failed to set clustered", Boolean.TRUE, configuration.isClustered());
   }

   public void testSetTransport() throws Exception
   {
      assertEquals(TransportType.TCP, configuration.getTransport());
   }

   public void testRemotingHost() throws Exception
   {
      assertEquals("localhost", configuration.getHost());
   }

   public void testSetRemotingPort() throws Exception
   {
      assertEquals(10000, configuration.getPort());

   }

   public void testSetRemotingTimeout() throws Exception
   {
      assertEquals(100, configuration.getTimeout());
   }

   public void testRemotingTcpNodelay() throws Exception
   {
      assertEquals(true, configuration.isTcpNoDelay());
   }

   public void testRemotingTcpReceiveBufferSize() throws Exception
   {
      assertEquals(8192, configuration.getTcpReceiveBufferSize());
   }

   public void testRemotingTcpSendBufferSize() throws Exception
   {
      assertEquals(1024, configuration.getTcpSendBufferSize());
   }

   public void testRemotingKeepAliveInterval() throws Exception
   {
      assertEquals(1234, configuration.getKeepAliveInterval());
   }

   public void testRemotingKeepAliveTimeout() throws Exception
   {
      assertEquals(5678, configuration.getKeepAliveTimeout());
   }

   public void testRemotingEnableSSL() throws Exception
   {
      assertEquals(true, configuration.isSSLEnabled());
   }

   public void testRemotingSSLKeyStorePath() throws Exception
   {
      assertEquals("messaging.keystore", configuration.getKeyStorePath());
   }

   public void testRemotingSSLKeyStorePassword() throws Exception
   {
      assertEquals("secureexample keystore", configuration.getKeyStorePassword());
   }

   public void testRemotingSSLTrustStorePath() throws Exception
   {
      assertEquals("messaging.truststore", configuration.getTrustStorePath());
   }

   public void testRemotingSSLTrustStorePassword() throws Exception
   {
      assertEquals("secureexample truststore", configuration.getTrustStorePassword());
   }

   public void testSetInterceptorsList() throws Exception
   {
      assertEquals("Didn't get the correct number of elements on interceptors", 2, configuration.getDefaultInterceptors().size());
      assertEquals("org.jboss.tst", configuration.getDefaultInterceptors().get(0));
      assertEquals("org.jboss.tst2", configuration.getDefaultInterceptors().get(1));
   }

   public void testMaxAIO() throws Exception
   {
      assertEquals(123, configuration.getJournalMaxAIO());
   }

   public void testAIOTimeout() throws Exception
   {
      assertEquals(123, configuration.getJournalAIOTimeout());
   }

   //config is supposed to be immutable??
//   public void testPropertyChangeListener() throws Exception
//   {
//      MyListener listener = new MyListener();
//      configuration.addPropertyChangeListener(listener);
//      listener.setCalled(false);
//      configuration.setMessageCounterSamplePeriod(1000000);
//      assertTrue("property change listener not fired", listener.isCalled());
//   }

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
