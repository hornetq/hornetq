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

import org.jboss.logging.Logger;
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

   private PersistenceManager pm;
   private Serializable channelID;

   // Constructors --------------------------------------------------

   public RecoverableState(Channel channel, PersistenceManager pm)
   {
      super(channel, true);
      if (pm == null)
      {
          throw new IllegalArgumentException("RecoverableState requires a non-null persistence manager");
      }
      this.pm = pm;

      // the channel isn't going to change, so cache its id
      this.channelID = channel.getChannelID();
   }

   // NonRecoverableState overrides -------------------------------------

   public boolean isRecoverable()
   {
      return true;
   }

   public void add(MessageReference ref, Transaction tx) throws Throwable
   {
      super.add(ref, tx);

      if (ref.isReliable())
      {
         //Reliable message in a recoverable state - also add to db
         if (log.isTraceEnabled()) { log.trace("adding " + ref + (tx == null ? " to database non-transactionally" : " in transaction: " + tx)); }
         pm.add(channelID, ref, tx);
      }

      if (tx != null)
      {
         addAddReferenceTask(tx);
         if (log.isTraceEnabled()) { log.trace("added an Add task to transaction " + tx); }
      }
   }

   public void addFirst(MessageReference ref) throws Throwable
   {
      super.addFirst(ref);

      if (ref.isReliable())
      {
         //Reliable message in a recoverable state - also add to db
         if (log.isTraceEnabled()) { log.trace("adding " + ref + " to database"); }
         // TODO Q1 - have a method that enforces ordering
         pm.add(channelID, ref, null);
         if (log.isTraceEnabled()) { log.trace("added " + ref + " to database"); }
      }
   }


   public boolean remove(MessageReference ref) throws Throwable
   {
      boolean memory = super.remove(ref);
      if (!memory)
      {
         return false;
      }

      if (ref.isReliable())
      {
         boolean database = pm.remove(channelID, ref);
         if (database && log.isTraceEnabled()) { log.trace("removed " + ref + " from database"); }
         return database;
      }

      return memory;
   }

   public MessageReference remove() throws Throwable
   {
      MessageReference removed = super.remove();
      if (removed == null)
      {
         return null;
      }

      if (removed.isReliable())
      {
         boolean database = pm.remove(channelID, removed);
         if (!database)
         {
            throw new IllegalStateException("reference " + removed + " not found in database");
         }
         else if (log.isTraceEnabled()) { log.trace("removed " + removed + " from database"); }
      }

      return removed;
   }

   public void add(Delivery d) throws Throwable
   {
      // Note! Adding of deliveries to the state is NEVER done in a transactional context.
      // The only things that are done in a transactional context are sending of messages and
      // removing deliveries (acking).

      super.add(d);

      if (d.getReference().isReliable())
      {
         // also add delivery to persistent storage (reliable delivery in recoverable state)
         pm.add(channelID, d);
         if (log.isTraceEnabled()) { log.trace("added " + d + " to database"); }
      }
   }

   public boolean remove(Delivery d, Transaction tx) throws Throwable
   {
      boolean memory = super.remove(d, tx);
      if (!memory)
      {
         return false;
      }

      if (d.getReference().isReliable())
      {
         boolean database = pm.remove(channelID, d, tx);
         if (database && log.isTraceEnabled()) { log.trace("removed " + d + " from database " + (tx == null ? "non-transactionally" : " on transaction " + tx)); }
         return database;
      }

      return memory;
   }

   public void clear()
   {
      super.clear();
      pm = null;
      // the persisted state remains
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
