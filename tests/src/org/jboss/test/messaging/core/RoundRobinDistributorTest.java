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
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.Distributor;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.Receiver;
import org.jboss.messaging.core.impl.RoundRobinDistributor;
import org.jboss.messaging.core.impl.SimpleDelivery;
import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.util.CoreMessageFactory;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1 $</tt>
 * $Id: $
 */
public class RoundRobinDistributorTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   public RoundRobinDistributorTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      
      ms = new SimpleMessageStore();
      
      ms.start();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
      
      ms.stop();
   }

   // Public --------------------------------------------------------
   
   public void testAllAccepting()
   {
      Distributor distributor = new RoundRobinDistributor();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         distributor.add(receivers[i]);
      }
      
      Message msg = CoreMessageFactory.createCoreMessage(123, true, null);
      
      MessageReference ref = ms.reference(msg);
            
      Delivery del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 2);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 5);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 6);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 7);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 8);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 9);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 2);
      resetReceivers(receivers);
      
   }
   
   public void testSomeClosed()
   {
      Distributor distributor = new RoundRobinDistributor();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         distributor.add(receivers[i]);
      }
      
      receivers[2].closed = true;
      
      receivers[5].closed = true;
      receivers[6].closed = true;
      
      receivers[9].closed = true;
      
      Message msg = CoreMessageFactory.createCoreMessage(123, true, null);
      
      MessageReference ref = ms.reference(msg);
      
      Delivery del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 7);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 8);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
      
   }
   
   public void testAllClosed()
   {
      Distributor distributor = new RoundRobinDistributor();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         receivers[i].closed = true;
         
         distributor.add(receivers[i]);
      }
      
      
      Message msg = CoreMessageFactory.createCoreMessage(123, true, null);
      
      MessageReference ref = ms.reference(msg);
      
      Delivery del = distributor.handle(null, ref, null);
      assertNull(del);

      del = distributor.handle(null, ref, null);
      assertNull(del);
      
      del = distributor.handle(null, ref, null);
      assertNull(del);

      
      
   }
   
   public void testSomeNoSelectorMatch()
   {
      Distributor distributor = new RoundRobinDistributor();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         distributor.add(receivers[i]);
      }
      
      receivers[2].selectorMatches = false;
      
      receivers[5].selectorMatches = false;
      receivers[6].selectorMatches = false;
      
      receivers[9].selectorMatches = false;
      
      Message msg = CoreMessageFactory.createCoreMessage(123, true, null);
      
      MessageReference ref = ms.reference(msg);
      
      Delivery del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 7);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 8);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 0);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 1);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 3);
      resetReceivers(receivers);
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      checkReceiverGotRef(receivers, 4);
      resetReceivers(receivers);
      
   }
   
   public void testAllNoSelectorMatch()
   {
      Distributor distributor = new RoundRobinDistributor();
      
      final int numReceivers = 10;
      
      SimpleReceiver[] receivers = new SimpleReceiver[numReceivers];
      
      for (int i = 0; i < numReceivers; i++)
      {
         receivers[i] = new SimpleReceiver();
         
         receivers[i].selectorMatches = false;
         
         distributor.add(receivers[i]);
      }
      
      
      Message msg = CoreMessageFactory.createCoreMessage(123, true, null);
      
      MessageReference ref = ms.reference(msg);
      
      Delivery del = distributor.handle(null, ref, null);
      assertNotNull(del);
      assertFalse(del.isSelectorAccepted());
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      assertFalse(del.isSelectorAccepted());
      
      del = distributor.handle(null, ref, null);
      assertNotNull(del);
      assertFalse(del.isSelectorAccepted());
           
   }
   
   public void testNoReceivers()
   {
      Distributor distributor = new RoundRobinDistributor();

      Message msg = CoreMessageFactory.createCoreMessage(123, true, null);
      
      MessageReference ref = ms.reference(msg);
      
      Delivery del = distributor.handle(null, ref, null);
      assertNull(del);
      
      del = distributor.handle(null, ref, null);
      assertNull(del);
      
      del = distributor.handle(null, ref, null);
      assertNull(del);    
   }


   /**
    * http://jira.jboss.org/jira/browse/JBMESSAGING-491
    */
   public void testDeadlock() throws Exception
   {
      final Distributor distributor = new RoundRobinDistributor();

      LockingReceiver receiver = new LockingReceiver();
      distributor.add(receiver);

      final Thread t = new Thread(new Runnable()
      {
         public void run()
         {
            // sends the message to the router on a separate thread
            
            Message msg = CoreMessageFactory.createCoreMessage(123, true, null);
            
            MessageReference ref = ms.reference(msg);
            
            distributor.handle(null, ref, null);
         }
      }, "Message sending thread");

      // start the sending tread, which will immediately grab the router's "receivers" lock, and it
      // will sleep for 3 seconds before attempting to grab LockingReceiver's lock.
      t.start();


      // in the mean time, the main thread immediately grabs receiver's lock ...

      synchronized(receiver.getLock())
      {
         // ... sleeps for 500 ms to allow sender thread time to grab router's "receivers" lock
         Thread.sleep(500);

         // ... and try to remove the receiver form router
         distributor.remove(receiver);
      }

      // normally, receiver removal should be immediate, as the router releases receiver's lock
      // immediately, so test should complete. Pre-JBMESSAGING-491, the test deadlocks.
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected void checkReceiverGotRef(SimpleReceiver[] receivers, int pos)
   {
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
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------  
   
   class LockingReceiver implements Receiver
   {
      private Object lock;

      public LockingReceiver()
      {
         lock = new Object();
      }

      public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
      {
         // The delivering thread needs to grab the receiver's lock to complete delivery; this
         // is how Messaging receivers are written, anyway. We simulate the race condition by
         // putting the sending thread to sleep for 3 seconds before allowing it to attempt to
         // grab the lock

         try
         {
            Thread.sleep(3000);
         }
         catch(InterruptedException e)
         {
            // this shouldn't happen in the test
            return null;
         }

         synchronized(lock)
         {
            return new SimpleDelivery(null, null, true, false);
         }
      }

      public Object getLock()
      {
         return lock;
      }
   }
   
   class SimpleReceiver implements Receiver
   {
      boolean selectorMatches = true;
      
      boolean closed;
      
      boolean gotRef;

      public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
      {
         if (closed)
         {
            return null;
         }
         
         Delivery del = new SimpleDelivery(null, null, selectorMatches, false);
         
         if (selectorMatches)
         {
            gotRef = true;
         }
                  
         return del;
      }
      
   }


   

}

