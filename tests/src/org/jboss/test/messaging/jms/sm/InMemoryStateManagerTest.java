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
package org.jboss.test.messaging.jms.sm;

import org.jboss.jms.server.InMemoryStateManager;
import org.jboss.jms.server.StateManager;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.util.id.GUID;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class InMemoryStateManagerTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------
   
   protected StateManager sm;
   
   // Constructors --------------------------------------------------

   public InMemoryStateManagerTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();
      
      ServerManagement.init("all");
      
      sm = createStateManager();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
      
      //ServerManagement.deInit();
   }
   
   public void testCreateGetRemoveDurableSubscription() throws Exception
   {
      String topicName = new GUID().toString();
      String clientID = new GUID().toString();
      String subscriptionName = new GUID().toString();
      String selector = new GUID().toString();
      
      ServerManagement.getServerPeer().getDestinationManager().createTopic(topicName);
            
      DurableSubscription sub = sm.createDurableSubscription(topicName, clientID, subscriptionName, selector);
      
      assertEquals(sub.getTopic().getName(), topicName);
      assertEquals(sub.getChannelID(), clientID + "." + subscriptionName);
      assertEquals(sub.getSelector(), selector);
      
      DurableSubscription sub_r = sm.getDurableSubscription(clientID, subscriptionName);
      
      assertEquals(sub_r.getTopic().getName(), topicName);
      assertEquals(sub_r.getChannelID(), clientID + "." + subscriptionName);
      assertEquals(sub_r.getSelector(), selector);
      
      boolean removed = sm.removeDurableSubscription(clientID, subscriptionName);
      assertTrue(removed);
      
      sub_r = sm.getDurableSubscription(clientID, subscriptionName);
      
      assertNull(sub_r);
      
      removed = sm.removeDurableSubscription(clientID, subscriptionName);
      assertFalse(removed);
        
   }
   
   public void testCreateGetRemoveDurableSubscriptionNullSelector() throws Exception
   {
      String topicName = new GUID().toString();
      String clientID = new GUID().toString();
      String subscriptionName = new GUID().toString();
      ServerManagement.getServerPeer().getDestinationManager().createTopic(topicName);
            
      DurableSubscription sub = sm.createDurableSubscription(topicName, clientID, subscriptionName, null);
      
      assertEquals(sub.getTopic().getName(), topicName);
      assertEquals(sub.getChannelID(), clientID + "." + subscriptionName);
      assertNull(sub.getSelector());      
      
      DurableSubscription sub_r = sm.getDurableSubscription(clientID, subscriptionName);
      
      assertEquals(sub_r.getTopic().getName(), topicName);
      assertEquals(sub_r.getChannelID(), clientID + "." + subscriptionName);
      assertNull(sub_r.getSelector());
      
      boolean removed = sm.removeDurableSubscription(clientID, subscriptionName);
      assertTrue(removed);
      
      sub_r = sm.getDurableSubscription(clientID, subscriptionName);
      
      assertNull(sub_r);
      
      removed = sm.removeDurableSubscription(clientID, subscriptionName);
      assertFalse(removed);
            
   }
   
   public void testGetPreConfClientId() throws Exception
   {
      String clientID = sm.getPreConfiguredClientID("blahblah");
      assertNull(clientID);
   }
   

   protected StateManager createStateManager() throws Exception
   {
      return new InMemoryStateManager(ServerManagement.getServerPeer());
   }
  
}



