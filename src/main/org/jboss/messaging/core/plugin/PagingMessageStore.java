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
package org.jboss.messaging.core.plugin;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.util.Util;
import org.jboss.system.ServiceMBeanSupport;

import javax.management.ObjectName;
import javax.management.MBeanServer;

/**
 * A MessageStore implementation that stores messages in an in-memory cache and page them on disk
 * as necessary.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PagingMessageStore extends ServiceMBeanSupport implements MessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PagingMessageStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private Serializable storeID;
   private boolean acceptReliableMessages;

   // <messageID - MessageHolder>
   private Map holders;

   private PersistenceManager pm;
   private ObjectName pmObjectName;

   // Constructors --------------------------------------------------

   /**
    * @param storeID - if more than one message store is to be used in a distributed messaging
    *        configuration, each store must have an unique store ID.
    */
   public PagingMessageStore(String storeID)
   {
      this.storeID = storeID;
      this.acceptReliableMessages = true;

      holders = new HashMap();

      log.debug(this + " initialized");
   }

   /**
    * Use this just for testing, in a container the persistence manager is injected during startup.
    */
   public PagingMessageStore(String storeID, PersistenceManager pm)
   {
      this(storeID);
      this.pm = pm;
   }

   // MessageStore implementation -----------------------------------

   public Object getInstance()
   {
      return this;
   }

   public Serializable getStoreID()
   {
      return storeID;
   }

   public boolean isRecoverable()
   {
      return pm != null;
   }

   public boolean acceptReliableMessages()
   {
      return acceptReliableMessages;
   }

   public synchronized MessageReference reference(Message m)
   {
      if (m.isReliable() && !acceptReliableMessages)
      {
         throw new IllegalStateException(this + " does not accept reliable messages (" + m + ")");
      }

      if (trace) { log.trace(this + " creating reference for " + m); }

      MessageHolder holder = (MessageHolder)holders.get(m.getMessageID());

      if (holder == null)
      {
         holder = new MessageHolder(m, this);
         holders.put(m.getMessageID(), holder);
      }
      return new SimpleMessageReference(holder, this);
   }

   //Will disappear once lazy loading is implemented
   public MessageReference reference(String messageID) throws Exception
   {
      if (trace) { log.trace("getting reference for message " + messageID);}

      // try and get the reference from the in memory cache first
      MessageHolder holder = (MessageHolder)holders.get(messageID);

      if (holder != null)
      {
         return new SimpleMessageReference(holder, this);
      }

      if (pm == null)
      {
         return null;
      }

      // Try and retrieve it from persistent storage
      // TODO We make a database trip even if the message is non-reliable, but I see no way to avoid
      // TODO this by only knowing the messageID ...

      //TODO - We would avoid this by storing the message header fields in the message reference table - Tim

      Message m = pm.getMessage((Serializable)messageID);

      if (m == null)
      {
         return null;
      }

      holder = new MessageHolder(m, this);
      holders.put(m.getMessageID(), holder);
      holder.setMessagePersisted(true);
      return new SimpleMessageReference(holder, this);
   }

   // JMX operations / attributes ------------------------------------

   /**
    * Managed attribute.
    */
   public void setPersistenceManager(ObjectName pmObjectName) throws Exception
   {
      this.pmObjectName = pmObjectName;
   }

   /**
    * Managed attribute.
    */
   public ObjectName getPersistenceManager()
   {
      return pmObjectName;
   }

   // ServiceMBeanSupport overrides ---------------------------------

   protected synchronized void startService() throws Exception
   {
      super.startService();

      MBeanServer server = getServer();

      pm = (PersistenceManager)server.getAttribute(pmObjectName, "Instance");
   }

   protected void stopService() throws Exception
   {
      log.debug(this + " stopped");
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "PagingStore[" + Util.guidToString(getStoreID()) + "]";
   }

   // Package protected ---------------------------------------------

   void forgetMessage(String messageID)
   {
      holders.remove(messageID);
   }

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
      
}
