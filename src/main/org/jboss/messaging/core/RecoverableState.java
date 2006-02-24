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
package org.jboss.messaging.core;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RecoverableState extends NonRecoverableState
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RecoverableState.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   private PersistenceManager pm;
   private Serializable channelID;
   private Serializable storeID;
   private MessageStore messageStore;
   
   // Constructors --------------------------------------------------

   public RecoverableState(Channel channel, PersistenceManager pm)
   {
      super(channel, true);
      if (pm == null)
      {
          throw new IllegalArgumentException("RecoverableState requires a " +
                                             "non-null persistence manager");
      }
      this.pm = pm;

      // the channel isn't going to change, so cache its id
      this.channelID = channel.getChannelID();
      
      this.storeID = channel.getMessageStore().getStoreID();
      
      this.messageStore = channel.getMessageStore();
        
   }

   // NonRecoverableState overrides -------------------------------------

   public boolean isRecoverable()
   {
      return true;
   }
   
   public void addReference(MessageReference ref, Transaction tx) throws Throwable
   {    
      super.addReference(ref, tx);

      if (ref.isReliable())
      {
         //Reliable message in a recoverable state - also add to db
         if (trace) { log.trace("adding " + ref + (tx == null ? " to database non-transactionally" : " in transaction: " + tx)); }
         pm.addReference(channelID, ref, tx);
      }
           
   }

   public boolean addReference(MessageReference ref) throws Throwable
   {  
      boolean first =  super.addReference(ref);

      if (ref.isReliable())
      {
         //Reliable message in a recoverable state - also add to db
         if (trace) { log.trace("adding " + ref + " to database non-transactionally"); }
         pm.addReference(channelID, ref, null);
      }      
      
      return first;
   }
   
   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {      
      super.acknowledge(d, tx);

      if (d.getReference().isReliable())
      {
         pm.removeReference(channelID, d.getReference(), tx);
      }     
   }
   
   public void acknowledge(Delivery d) throws Throwable
   {   
      super.acknowledge(d);

      //TODO - Optimisation - If the message is acknowledged before the call to handle() returns
      //And it is a new message then there won't be any reference in the database
      //So the call to remove from the db is wasted.
      //We should add a flag to check this
      if (d.getReference().isReliable())
      {
         pm.removeReference(channelID, d.getReference(), null);
      }     
   }
   
   public void clear()
   {
      super.clear();
      pm = null;
      // the persisted state remains
   }
   
   /**
    * Load the state from persistent storage
    * 
    * This will all change when we implement lazy loading
    */
   public void load() throws Exception
   {
      List refs = pm.messageRefs(storeID, channelID);
      
      Iterator iter = refs.iterator();
      while (iter.hasNext())
      {
         String messageID = (String)iter.next();
         
         MessageReference ref = messageStore.reference(messageID);
         
         messageRefs.addLast(ref, ref.getPriority());     
         
         ref.incChannelCount();         
      }        
   }

   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------
   protected void removeCompletely(MessageReference r)
   {
      if (r.isReliable())
      {
         try 
         {
            pm.removeReference(channelID, r, null);
         }
         catch (Exception e)
         {
            if (trace) { log.trace("removeAll() failed on removing " + r, e); }   
         }
      }
      super.removeCompletely(r);
   }
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------   
}
