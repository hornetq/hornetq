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
package org.jboss.messaging.core.contract;

import java.util.List;

/**
 * A Channel is a transactional, reliable message delivery mechanism that forwards a message from a
 * sender to one or more receivers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Channel extends DeliveryObserver, Receiver
{
   /**    
    * @return the unique ID of the channel
    */
   long getChannelID();

   /**
    * @return true if the channel can guarantee recoverability for <i>reliable</i> messages.
    *         Recoverability is not guaranteed for non-reliable messages even if the channel is recoverable.
    */
   boolean isRecoverable();

   /**
    * @param filter - may be null, in which case no filter is applied.
    *
    * @return a List containing message references of messages whose state is maintained by this
    *         State instance. 
    */
   List browse(Filter filter);

   /**
    * Delivers as many references as possible to its router until receivers will accept no more
    */
   void deliver();

   /**
    * Close the channel
    */
   void close();

   /**
    * @return Total message count = undelivered + delivering + scheduled
    */
   int getMessageCount();
   
   /**
    * @return Count being delivered
    */
   int getDeliveringCount();
   
   /**
    * Count scheduled for delivery
    */
   int getScheduledCount();
   
   /**
    * Remove all the references in the channel
    * @throws Throwable
    */
   void removeAllReferences() throws Throwable;
   
   /**
    * Load any references for this channel from storage
    * @throws Exception
    */
   void load() throws Exception;
   
   /**
    * Unload any references for this channel
    * @throws Exception
    */
   void unload() throws Exception;
   
   /**
    * Activate the channel.
    */
   void activate();
   
   /**
    * Deactivate the channel
    */
   void deactivate();
   
   /**
    * @return true if the channel is active
    */
   boolean isActive();
   
   /**
    * Given a List of message ids, create a list of deliveries for them
    * @param messageIds
    * @return
    */
   List recoverDeliveries(List messageIds);
  
   /**
    * 
    * @return The maxiumum number of references this channel can store
    */
   int getMaxSize();
   
   /**
    * Set the maximum number of references this channel can store
    * @param newSize
    */
   void setMaxSize(int newSize);
   
   /**
    * Get the total number of messages added since this channel was started
    * @return
    */
   int getMessagesAdded();
}


