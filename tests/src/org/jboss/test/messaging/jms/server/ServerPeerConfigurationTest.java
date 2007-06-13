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
import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.jboss.test.messaging.tools.jmx.ServiceAttributeOverrides;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.tools.jmx.rmi.LocalTestServer;

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
   
   /**
    * Test that the ServerPeer can be configured without using
    * the &lt;constructor&gt; element, only through attributes.
    */
   public void testConstructorlessConfiguration() throws Exception
   {
      MyLocalTestServer server = new MyLocalTestServer();
      server.start("all", false);
      server.stop();
   }
   
   public void testServerPeerID() throws Exception
   {
      testConstructorArgumentAttribute("ServerPeerID", new Integer(5), new Integer(10));
   }

   public void testDefaultQueueJNDIContext() throws Exception
   {
      testConstructorArgumentAttribute("DefaultQueueJNDIContext", "/myqueues", "/otherqueues");
   }

   public void testDefaultTopicJNDIContext() throws Exception
   {
      testConstructorArgumentAttribute("DefaultTopicJNDIContext", "/mytopics", "/othertopics");
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void testConstructorArgumentAttribute(String attributeName,
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
   
   /**
    * Configures ServerPeer through attributes instead of constructor
    * arguments.
    */
   private class MyLocalTestServer extends LocalTestServer
   {
      protected void overrideServerPeerConfiguration(MBeanConfigurationElement config,
            int serverPeerID, String defaultQueueJNDIContext, String defaultTopicJNDIContext)
         throws Exception
      {
         config.removeConstructors();
         
         config.setAttribute("ServerPeerID", Integer.toString(serverPeerID));
         config.setAttribute("DefaultQueueJNDIContext",
               defaultQueueJNDIContext == null ? "/queue" : defaultQueueJNDIContext);
         config.setAttribute("DefaultTopicJNDIContext",
               defaultTopicJNDIContext == null? "/topic" : defaultTopicJNDIContext);
      }
      
      // Make public
      public ServiceContainer getServiceContainer()
      {
         return super.getServiceContainer();
      }
   }
}
