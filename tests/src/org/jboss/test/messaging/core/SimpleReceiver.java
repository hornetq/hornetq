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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TxCallback;
import org.jboss.util.id.GUID;

/**
 * A simple Receiver implementation that consumes undelivered by storing them internally. Used for
 * testing. The receiver can be configured to immediately return a "done" delivery (ACKING),
 * an "active" delivery (NACKING) undelivered, or throw unchecked exceptions.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleReceiver implements Receiver
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SimpleReceiver.class);

   public static final String ACKING = "ACKING";
   public static final String ACCEPTING = "ACCEPTING";
   public static final String BROKEN = "BROKEN";
   public static final String REJECTING = "REJECTING";
   public static final String SELECTOR_REJECTING = "SELECTOR_REJECTING";
   public static final String ACCEPTING_TO_MAX = "ACCEPTING_TO_MAX";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // <Object[2] { Routable, Delivery }>
   private List messages;
   private String state;
   private String name;
   private Channel channel; 
   
   private boolean immediateAsynchronousAcknowledgment;
   private int maxRefs;
   
   private int count;
   private int waitForCount = -1;
   

   // Constructors --------------------------------------------------

   public SimpleReceiver()
   {
      this(ACKING);
   }

   public SimpleReceiver(String name)
   {
      this(name, ACKING);
   }

   /**
    *
    * @param name
    * @param state:
    *        ACKING - the receiver returns synchronously a "done" delivery.
    *        NACKING - the receiver returns an active delivery, and has the option of acking it later
    *        BROKEN - throws exception
    */
   public SimpleReceiver(String name, String state)
   {
      this(name, state, null);
   }
   

   public SimpleReceiver(String name, String state, Channel channel)
   {
      checkValid(state);

      this.name = name;
      this.state = state;
      this.channel = channel;
      messages = new ArrayList();
      immediateAsynchronousAcknowledgment = false;
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
   {
      log.trace(this + " got routable:" + ref);
          
      try
      {
         if (ref == null)
         {
            log.trace("Receiver [" + name + "] is rejecting a null reference");
            return null;
         }
         
         if (SELECTOR_REJECTING.equals(state))
         {
            log.trace(this + " is rejecting message since doesn't match selector");
            return new SimpleDelivery(null, null, true, false);
         }

         if (REJECTING.equals(state))
         {

            log.trace(this + " is rejecting reference " + ref);
            return null;
         }
         
         if (ACCEPTING_TO_MAX.equals(state))
         {
            //Only accept up to maxRefs references
            if (messages.size() == maxRefs)
            {
               return null;
            }
         }

         if (BROKEN.equals(state))
         {
            throw new RuntimeException("THIS IS AN EXCEPTION THAT SIMULATES "+
                                       "THE BEHAVIOUR OF A BROKEN RECEIVER");
         }

         log.trace("State is:" + state);
         
         boolean done = ACKING.equals(state);
         
         //NOTE! it is NOT Nacking, it is keeping - don't say NACKing - it is misleading (nack means cancel)         
         log.trace(this + " is " + (done ? "ACKing" : "Keeping") +  " message " + ref);
         
         Message m = ref.getMessage();
         
         SimpleDelivery delivery = new SimpleDelivery(observer, ref, done);
         messages.add(new Object[] {m, done ? null : delivery});
         
         if (immediateAsynchronousAcknowledgment)
         {
            log.trace("simulating an asynchronous ACK that arrives before we return the delivery to channel");
            try
            {
               delivery.acknowledge(null);
            }
            catch(Throwable t)
            {
               log.error("Cannot acknowledge", t);
            }
         }
         return delivery;
      }
      finally
      {
         synchronized (this)
         {
            count++;
            if (waitForCount != -1 && count >= waitForCount)
            {
               this.notify();
            }
         }         
      }
   }
   
   // Public --------------------------------------------------------
   
   public void setMaxRefs(int max)
   {
      this.maxRefs = max;
   }

   public void setImmediateAsynchronousAcknowledgment(boolean b)
   {
      immediateAsynchronousAcknowledgment = b;
   }

   public String getName()
   {
      return name;
   }

   public void requestMessages()
   {
      if (channel == null)
      {
         log.error("No channel, cannot request messages");
         return;
      }
      log.trace("receiver explicitely requesting message from the channel");
      channel.deliver();
   }

   public void clear()
   {
      messages.clear();
   }

   public List getMessages()
   {
      List l = new ArrayList();
      for (Iterator i = messages.iterator(); i.hasNext(); )
      {
         Object[] o = (Object[])i.next();
         l.add(o[0]);
      }
      return l;
   }

   /**
    * Blocks until handle() is called for the specified number of times.
    *
    * @return true if the handle was invoked the specified number of times or false if the method
    *         exited with timeout.
    */
   public boolean waitForHandleInvocations(int waitFor, long timeout)
   {
      long start = System.currentTimeMillis();
      
      synchronized(this)
      {
         this.waitForCount = waitFor;
         
         while (this.count < waitForCount)
         {      
            if (timeout < 0)
            {
               log.trace(this + ".waitForHandleInvocations() current timeout is " + timeout);
               resetInvocationCount();
               return false;
            }
            
            try
            {
               this.wait(timeout);
               long now = System.currentTimeMillis();
               timeout -= now - start;
               start = now;
            }
            catch(InterruptedException e)
            {
               log.debug(e);
            }
         }
      }
      
      resetInvocationCount();
      return true;
   }

   public void acknowledge(Message r, Transaction tx) throws Throwable
   {
      log.debug(this + " acknowledging "  + r);

      Object[] touple = null;
      Delivery d = null;
      for (Iterator i = messages.iterator(); i.hasNext(); )
      {
         Object[] o = (Object[])i.next();
         Message m = (Message)o[0];
         if (m == r)
         {
            log.trace("*** found it");
            d = (Delivery)o[1];
            touple = o;
            break;
         }
      }

      if (touple == null)
      {
         throw new IllegalStateException("The message " + r + " hasn't been received yet!");
      }

      if (d == null)
      {
         throw new IllegalStateException("The message " + r + " has already been acknowledged!");
      }

      d.acknowledge(tx);

      log.trace(this + " acknowledged "  + r);

      // make sure I get rid of message if the transaction is rolled back
      if (tx != null)
      {
         tx.addCallback(new PostAcknowledgeCommitCallback(touple), new GUID().toString());
      }
   }

   public void cancel(Message r) throws Throwable
   {
      Object[] touple = null;
      Delivery d = null;
      for (Iterator i = messages.iterator(); i.hasNext(); )
      {
         Object[] o = (Object[])i.next();
         Message m = (Message)o[0];
         if (m == r)
         {
            d = (Delivery)o[1];
            touple = o;
            i.remove();
            break;
         }
      }

      if (touple == null)
      {
         throw new IllegalStateException("The message " + r + " hasn't been received yet!");
      }

      if (d == null)
      {
         throw new IllegalStateException("The message " + r + " has already been acknowledged!");
      }

      d.cancel();

      log.trace(this + " cancelled "  + r);
   }

   public String toString()
   {
      return "Receiver["+ name +"](" + state + ")";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private static void checkValid(String state)
   {
      if (!ACKING.equals(state) &&
          !ACCEPTING.equals(state) &&
          !BROKEN.equals(state) &&
          !REJECTING.equals(state) &&
          !SELECTOR_REJECTING.equals(state) &&
          !ACCEPTING_TO_MAX.equals(state))
      {
         throw new IllegalArgumentException("Unknown receiver state: " + state);
      }
   }
   
   private void resetInvocationCount()
   {
     this.waitForCount = -1;
     this.count = 0;      
   }

   // Inner classes -------------------------------------------------

   private class PostAcknowledgeCommitCallback implements TxCallback
   {
      private Object[] touple;


      /**
       * @param touple - touple[0] contains the message, touple[1] contains the delivery
       */
      public PostAcknowledgeCommitCallback(Object[] touple)
      {
         this.touple = touple;
      }

      public void afterRollback(boolean onePhase)
      {
         
      }
      
      public void afterCommit()
      {
         // clear the delivery
         touple[1] = null;
      }

      public void afterCommit(boolean onePhase) throws Exception
      {
         
      }

      public void afterPrepare() throws Exception
      {
         
      }

      public void beforeCommit(boolean onePhase) throws Exception
      {
 
      }

      public void beforePrepare() throws Exception
      {
   
      }

      public void beforeRollback(boolean onePhase) throws Exception
      {

      }
   }
}
