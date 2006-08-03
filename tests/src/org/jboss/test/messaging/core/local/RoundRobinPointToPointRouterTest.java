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
package org.jboss.test.messaging.core.local;

import java.util.Set;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Router;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.local.RoundRobinPointToPointRouter;
import org.jboss.messaging.core.plugin.SimpleMessageReference;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.test.messaging.MessagingTestCase;


public class RoundRobinPointToPointRouterTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public RoundRobinPointToPointRouterTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Public --------------------------------------------------------
   
   public void testAllAccepting()
   {
      Router router = new RoundRobinPointToPointRouter();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         router.add(receivers[i]);
      }
      
      MessageReference ref = new SimpleMessageReference();
      
      Set dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 2);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 5);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 6);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 7);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 8);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 9);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 2);
      resetReceivers(receivers);
      
   }
   
   public void testSomeClosed()
   {
      Router router = new RoundRobinPointToPointRouter();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         router.add(receivers[i]);
      }
      
      receivers[2].closed = true;
      
      receivers[5].closed = true;
      receivers[6].closed = true;
      
      receivers[9].closed = true;
      
      MessageReference ref = new SimpleMessageReference();
      
      Set dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 7);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 8);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
      
   }
   
   public void testAllClosed()
   {
      Router router = new RoundRobinPointToPointRouter();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         receivers[i].closed = true;
         
         router.add(receivers[i]);
      }
      
      
      MessageReference ref = new SimpleMessageReference();
      
      Set dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(0, dels.size());
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(0, dels.size());
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(0, dels.size());
      
      
   }
   
   public void testSomeNoSelectorMatch()
   {
      Router router = new RoundRobinPointToPointRouter();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         router.add(receivers[i]);
      }
      
      receivers[2].selectorMatches = false;
      
      receivers[5].selectorMatches = false;
      receivers[6].selectorMatches = false;
      
      receivers[9].selectorMatches = false;
      
      MessageReference ref = new SimpleMessageReference();
      
      Set dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 7);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 8);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
   }
   
   public void testAllNoSelectorMatch()
   {
      Router router = new RoundRobinPointToPointRouter();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         receivers[i].selectorMatches = false;
         
         router.add(receivers[i]);
      }
      
      
      MessageReference ref = new SimpleMessageReference();
      
      Set dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(1, dels.size());
      assertFalse(((SimpleDelivery)dels.iterator().next()).isSelectorAccepted());
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertFalse(((SimpleDelivery)dels.iterator().next()).isSelectorAccepted());
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertFalse(((SimpleDelivery)dels.iterator().next()).isSelectorAccepted());
           
   }
   
   public void testNoReceivers()
   {
      Router router = new RoundRobinPointToPointRouter();

      MessageReference ref = new SimpleMessageReference();
      
      Set dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(0, dels.size());
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(0, dels.size());
      
      dels = router.handle(null, ref, null);
      assertNotNull(dels);
      assertEquals(0, dels.size());     
   }
   
   protected void checkReceiverGotRef(SimpleReceiver[] receivers, int pos)
   {
      log.info("checkReceiverGotRef:" + pos);
      for (int i = 0; i < receivers.length; i++)
      {
         SimpleReceiver r = receivers[i];
         
         if (i == pos)
         {
            assertTrue(r.gotRef);
         }
         else
         {
            assertFalse(r.gotRef);
         }
      }
   }
   
   protected void resetReceivers(SimpleReceiver[] receivers)
   {
      for (int i = 0; i < receivers.length; i++)
      {
         SimpleReceiver r = receivers[i];
         
         r.gotRef = false;
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
   
   class SimpleReceiver implements Receiver
   {
      boolean selectorMatches = true;
      
      boolean closed;
      
      boolean gotRef;

      public Delivery handle(DeliveryObserver observer, Routable routable, Transaction tx)
      {
         if (closed)
         {
            return null;
         }
         
         Delivery del = new SimpleDelivery(null, null, true, selectorMatches);
         
         if (selectorMatches)
         {
            gotRef = true;
         }
                  
         return del;
      }
      
   }
}

