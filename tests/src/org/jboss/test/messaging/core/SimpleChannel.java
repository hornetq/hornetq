/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core;

import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Iterator;
import java.io.Serializable;

/**
 * A test Channel implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleChannel implements Channel
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SimpleChannel.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String channelID;
   private MessageStore ms;
   private boolean deliveryNotification = false;

   // Constructors --------------------------------------------------

   public SimpleChannel(String channelID, MessageStore ms)
   {
      this.channelID = channelID;
      this.ms = ms;
   }

   // Channel implementation ----------------------------------------

   public Serializable getChannelID()
   {
      return channelID;
   }

   public boolean isRecoverable()
   {
      throw new NotYetImplementedException();
   }

   public boolean acceptReliableMessages()
   {
      throw new NotYetImplementedException();
   }

   public List browse()
   {
      throw new NotYetImplementedException();
   }

   public List browse(Filter filter)
   {
      throw new NotYetImplementedException();
   }

   public MessageStore getMessageStore()
   {
      return ms;
   }

   public void deliver()
   {
      log.trace("");
      deliveryNotification = true;
   }

   public void deliver(Receiver receiver)
   {
      throw new NotYetImplementedException();
   }

   public void close()
   {
      throw new NotYetImplementedException();
   }

   // DeliveryObserver implementation -------------------------------

   public void acknowledge(Delivery d, Transaction tx)
   {
      throw new NotYetImplementedException();
   }

   public boolean cancel(Delivery d) throws Throwable
   {
      throw new NotYetImplementedException();
   }

   public void redeliver(Delivery old, Receiver r) throws Throwable
   {
      throw new NotYetImplementedException();
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver observer, Routable routable, Transaction tx)
   {
      throw new NotYetImplementedException();
   }

   // Distributor implementation ------------------------------------

   public boolean contains(Receiver receiver)
   {
      throw new NotYetImplementedException();
   }

   public Iterator iterator()
   {
      throw new NotYetImplementedException();
   }

   public boolean add(Receiver receiver)
   {
      throw new NotYetImplementedException();
   }

   public boolean remove(Receiver receiver)
   {
      throw new NotYetImplementedException();
   }

   public void clear()
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   public void reset()
   {
      deliveryNotification = false;
   }

   public boolean wasNotifiedToDeliver()
   {
      return deliveryNotification;
   }

   public String toString()
   {
      return "SimpleChannel[" + getChannelID() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
