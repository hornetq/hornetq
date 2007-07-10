/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.test.messaging.core;

import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Channel;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.Filter;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.Receiver;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.messaging.util.NotYetImplementedException;

/**
 * A test Channel implementation.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   private long channelID;
   private MessageStore ms;
   private boolean deliveryNotification = false;

   // Constructors --------------------------------------------------

   public SimpleChannel(long channelID, MessageStore ms)
   {
      this.channelID = channelID;
      this.ms = ms;
   }

   // Channel implementation ----------------------------------------

   public long getChannelID()
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
      log.debug("deliver()");
      deliveryNotification = true;
   }
   

   public void close()
   {
      throw new NotYetImplementedException();
   }
   
   
   public void add(Delivery delivery)
   {
      throw new NotYetImplementedException();
   }
   
   public void removeAllReferences()
   {      
   }
  

   // DeliveryObserver implementation -------------------------------

   public void acknowledge(Delivery d, Transaction tx)
   {
      throw new NotYetImplementedException();
   }

   public void cancel(Delivery d) throws Exception
   {
      throw new NotYetImplementedException();
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
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

   public List delivering(Filter filter)
   {
      throw new NotYetImplementedException();
   }

   public void load() throws Exception
   { 
      throw new NotYetImplementedException();
   }

   public int getMessageCount()
   {
      throw new NotYetImplementedException();
   }

   public List undelivered(Filter filter)
   {
      throw new NotYetImplementedException();
   }

   public int getNumberOfReceivers()
   {
      throw new NotYetImplementedException();
   }

   public void activate()
   {
      throw new NotYetImplementedException();
   }

   public void deactivate()
   {
      throw new NotYetImplementedException();
   }

   public void unload() throws Exception
   {
      throw new NotYetImplementedException();
   }
   
   public boolean isActive()
   {
      throw new UnsupportedOperationException();
   }

   public List recoverDeliveries(List messageIds)
   {
      throw new UnsupportedOperationException();
   }

   public void addDelivery(Delivery del)
   {
      throw new UnsupportedOperationException();
   }

   public int getDeliveringCount()
   {
      throw new UnsupportedOperationException();
   }

   public int getMaxSize()
   {
      throw new UnsupportedOperationException();
   }

   public int getMessagesAdded()
   {
      throw new UnsupportedOperationException();
   }

   public void setMaxSize(int newSize)
   {
      throw new UnsupportedOperationException();
   }

   public int getScheduledCount()
   {
      throw new UnsupportedOperationException();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
