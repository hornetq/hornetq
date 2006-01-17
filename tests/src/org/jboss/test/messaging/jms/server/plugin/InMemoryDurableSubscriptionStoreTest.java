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
package org.jboss.test.messaging.jms.server.plugin;

import org.jboss.jms.server.plugin.contract.DurableSubscriptionStoreDelegate;
import org.jboss.jms.server.ServerPeer;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.util.id.GUID;


/**
 * These tests must not be ran in remote mode!
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class InMemoryDurableSubscriptionStoreTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------
   
   protected DurableSubscriptionStoreDelegate dssd;
   
   // Constructors --------------------------------------------------

   public InMemoryDurableSubscriptionStoreTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("This test must not be ran in remote mode!");
      }

      super.setUp();

      ServerManagement.start("all");
      dssd = createStateManager(ServerManagement.getServerPeer());
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   public void testCreateGetRemoveDurableSubscription() throws Exception
   {
      String topicName = new GUID().toString();
      String clientID = new GUID().toString();
      String subscriptionName = new GUID().toString();
      String selector = new GUID().toString();
      
      ServerManagement.deployTopic(topicName);
            
      DurableSubscription sub = dssd.createDurableSubscription(topicName, clientID, subscriptionName, selector);
      
      assertEquals(sub.getTopic().getName(), topicName);
      assertEquals(sub.getChannelID(), clientID + "." + subscriptionName);
      assertEquals(sub.getSelector(), selector);
      
      DurableSubscription sub_r = dssd.getDurableSubscription(clientID, subscriptionName);
      
      assertEquals(sub_r.getTopic().getName(), topicName);
      assertEquals(sub_r.getChannelID(), clientID + "." + subscriptionName);
      assertEquals(sub_r.getSelector(), selector);
      
      boolean removed = dssd.removeDurableSubscription(clientID, subscriptionName);
      assertTrue(removed);
      
      sub_r = dssd.getDurableSubscription(clientID, subscriptionName);
      
      assertNull(sub_r);
      
      removed = dssd.removeDurableSubscription(clientID, subscriptionName);
      assertFalse(removed);
        
   }
   
   public void testCreateGetRemoveDurableSubscriptionNullSelector() throws Exception
   {
      String topicName = new GUID().toString();
      String clientID = new GUID().toString();
      String subscriptionName = new GUID().toString();

      ServerManagement.deployTopic(topicName);
            
      DurableSubscription sub = dssd.createDurableSubscription(topicName, clientID, subscriptionName, null);
      
      assertEquals(sub.getTopic().getName(), topicName);
      assertEquals(sub.getChannelID(), clientID + "." + subscriptionName);
      assertNull(sub.getSelector());      
      
      DurableSubscription sub_r = dssd.getDurableSubscription(clientID, subscriptionName);
      
      assertEquals(sub_r.getTopic().getName(), topicName);
      assertEquals(sub_r.getChannelID(), clientID + "." + subscriptionName);
      assertNull(sub_r.getSelector());
      
      boolean removed = dssd.removeDurableSubscription(clientID, subscriptionName);
      assertTrue(removed);
      
      sub_r = dssd.getDurableSubscription(clientID, subscriptionName);
      
      assertNull(sub_r);
      
      removed = dssd.removeDurableSubscription(clientID, subscriptionName);
      assertFalse(removed);
            
   }
   
   public void testGetPreConfClientId() throws Exception
   {
      String clientID = dssd.getPreConfiguredClientID("blahblah");
      assertNull(clientID);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected DurableSubscriptionStoreDelegate createStateManager(ServerPeer serverPeer)
      throws Exception
   {
     InMemorySubscriptionStore s = new InMemorySubscriptionStore();
     s.setServerPeer(serverPeer);
     return s; 
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}



