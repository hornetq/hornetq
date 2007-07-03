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
package org.jboss.test.messaging.jms.util;

import junit.framework.TestCase;

import org.jboss.messaging.util.MessageQueueNameHelper;


public class MessageQueueNameHelperTest extends TestCase
{   
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public MessageQueueNameHelperTest(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------

   public void testQueueName()
   {
      String name = "clientid123.mysub";
      
      MessageQueueNameHelper helper = MessageQueueNameHelper.createHelper(name);
      
      assertEquals("clientid123", helper.getClientId());
      assertEquals("mysub", helper.getSubName());
   }
   
   public void testCreate()
   {
      String name = MessageQueueNameHelper.createSubscriptionName("clientid456", "mysub2");
      
      assertEquals("clientid456.mysub2", name);
   }
   
   public void testEscaping()
   {
      String clientID = ".client.id.";
      String subscriptionName = ".subscription.name.";
      
      String queueName = MessageQueueNameHelper.createSubscriptionName(clientID, subscriptionName);
      assertEquals("\\.client\\.id\\..\\.subscription\\.name\\.", queueName);
      
      MessageQueueNameHelper helper = MessageQueueNameHelper.createHelper(queueName);
      
      assertEquals(clientID, helper.getClientId());
      assertEquals(subscriptionName, helper.getSubName());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}


