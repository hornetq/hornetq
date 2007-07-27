/*
 * JBoss, Home of Professional Open Source
 * Copyright 2007, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.jms.server;

import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.LocalTestServer;
import org.jboss.test.messaging.tools.container.ServiceAttributeOverrides;
import org.jboss.test.messaging.tools.container.ServiceContainer;

/**
 * Test ServerPeer configuration.
 *
 * @author <a href="sergey.koshcheyev@jboss.com">Sergey Koshcheyev</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class ServerPeerConfigurationTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------
   
   public ServerPeerConfigurationTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
   
   public void setUp() throws Exception
   {
   	ServerManagement.stop();
   }
   
   public void tearDown() throws Exception
   {   	
   }
   
   public void testServerPeerID() throws Exception
   {
      testStartupOnlyAttribute("ServerPeerID", new Integer(5), new Integer(10));
   }

   public void testDefaultQueueJNDIContext() throws Exception
   {
      testStartupOnlyAttribute("DefaultQueueJNDIContext", "/myqueues", "/otherqueues");
   }

   public void testDefaultTopicJNDIContext() throws Exception
   {
      testStartupOnlyAttribute("DefaultTopicJNDIContext", "/mytopics", "/othertopics");
   }
   
   public void testCannotSetNegativeServerPeerID() throws Exception
   {
      LocalTestServer server = new LocalTestServer();
      ServiceAttributeOverrides overrides = new ServiceAttributeOverrides();
      // Can't use server.getServerPeerObjectName() here since it's not known to the server yet.
      overrides.put(ServiceContainer.SERVER_PEER_OBJECT_NAME, "ServerPeerID", "-10");
      
      try
      {
         server.start("all", overrides, false, true);
         fail("Should have thrown an exception when setting ServerPeerID to a negative value");
      }
      catch (RuntimeMBeanException rmbe)
      {
         assertTrue(rmbe.getCause() instanceof IllegalArgumentException);
      }
      finally
      {
      	server.stop();
      }
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void testStartupOnlyAttribute(String attributeName,
         Object initialAttributeValue, Object anotherAttributeValue) throws Exception
   {
      MyLocalTestServer server = new MyLocalTestServer();
      ServiceAttributeOverrides overrides = new ServiceAttributeOverrides();
      // Can't use server.getServerPeerObjectName() here since it's not known to the server yet.
      overrides.put(ServiceContainer.SERVER_PEER_OBJECT_NAME, attributeName, initialAttributeValue.toString());
      
      server.start("all", overrides, false, true);
      try
      {
         ObjectName sp = server.getServerPeerObjectName();
         Object actualValue = server.getServiceContainer()
            .getAttribute(server.getServerPeerObjectName(), attributeName);

         assertEquals(initialAttributeValue, actualValue);

         try
         {
            server.getServiceContainer().setAttribute(sp, attributeName, anotherAttributeValue.toString());
            fail("Should throw an exception when setting " + attributeName + " after startup");
         }
         catch (RuntimeMBeanException e)
         {
            assertTrue(e.getCause() instanceof IllegalStateException);
         }
      }
      finally
      {
         server.stop();
      }
   }

   // Inner classes -------------------------------------------------
   
   private class MyLocalTestServer extends LocalTestServer
   {
      // Make accessible from the test
      protected ServiceContainer getServiceContainer()
      {
         return super.getServiceContainer();
      }
   }
}
