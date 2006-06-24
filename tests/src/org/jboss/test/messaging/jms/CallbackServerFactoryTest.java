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
package org.jboss.test.messaging.jms;

import org.jboss.jms.client.remoting.CallbackServerFactory;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;
import org.jboss.test.messaging.MessagingTestCase;

/**
 * 
 * A CallbackServerFactoryTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class CallbackServerFactoryTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public CallbackServerFactoryTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
   
   public void testCallbackServer() throws Exception
   {
      String locatorURI1 = "socket://localhost:1234";
      
      String locatorURI2 = "sslsocket://localhost:4321";
      
      InvokerLocator locator1 = new InvokerLocator(locatorURI1);
      
      InvokerLocator locator2 = new InvokerLocator(locatorURI2);
      
      Connector server1 = CallbackServerFactory.instance.getCallbackServer(locator1);
      
      assertTrue(server1.isStarted());
      
      assertTrue(CallbackServerFactory.instance.containsCallbackServer(locator1.getProtocol()));
      
      Connector server2 = CallbackServerFactory.instance.getCallbackServer(locator1);
      
      assertTrue(server1 == server2);
      
      Connector server3 = CallbackServerFactory.instance.getCallbackServer(locator2);
      
      assertTrue(server3.isStarted());
      
      assertTrue(CallbackServerFactory.instance.containsCallbackServer(locator2.getProtocol()));
      
      Connector server4 = CallbackServerFactory.instance.getCallbackServer(locator2);
      
      assertTrue(server3 == server4);
      
      assertFalse(server1 == server3);
      
      
      CallbackServerFactory.instance.returnCallbackServer(locator1.getProtocol());
      
      assertTrue(CallbackServerFactory.instance.containsCallbackServer(locator1.getProtocol()));
      
      CallbackServerFactory.instance.returnCallbackServer(locator2.getProtocol());
      
      assertTrue(CallbackServerFactory.instance.containsCallbackServer(locator2.getProtocol()));
      
      CallbackServerFactory.instance.returnCallbackServer(locator1.getProtocol());
      
      assertFalse(CallbackServerFactory.instance.containsCallbackServer(locator1.getProtocol()));
      
      CallbackServerFactory.instance.returnCallbackServer(locator2.getProtocol());
      
      assertFalse(CallbackServerFactory.instance.containsCallbackServer(locator2.getProtocol()));
      
      assertFalse(server1.isStarted());
      
      assertFalse(server3.isStarted());
      
      
   }

   
   // Inner classes -------------------------------------------------

}

