/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.message;

import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Message;
import org.jboss.logging.Logger;

import javax.transaction.TransactionManager;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PersistentMessageStore extends TransactionalMessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PersistentMessageStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private PersistenceManager pm;

   // Constructors --------------------------------------------------

   public PersistentMessageStore(Serializable storeID, PersistenceManager pm, TransactionManager tm)
   {
      super(storeID, tm);
      this.pm = pm;
   }

   // MessageStore overrides ----------------------------------------

   public boolean isReliable()
   {
      return true;
   }

   public MessageReference reference(Routable r) throws Throwable
   {
      //we always put it in the memory cache first - whether it's reliable or not
      MessageReference ref = super.reference(r);

      if (log.isTraceEnabled()) { log.trace("Persisting message " + r); }

      if (r.isReliable())
      {      
         //and in the persistent store if it's reliable
         pm.store((Message)r);
         
         if (log.isTraceEnabled()) { log.trace("Persisted message " + r); }
      }
      
      return ref;
   }

   
   public MessageReference getReference(Serializable messageID) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Getting message ref for message ID: " + messageID);}
      
      //Try and get the reference from the in memory cache first
      MessageReference ref = super.getReferenceInternal(messageID);
      
      if (ref != null)
      {        
         if (log.isTraceEnabled()) { log.trace("Retrieved it from memory cache"); }
         return ref;
      }

      //Try and retrieve it from persistent storage
      Message m = null;
      try
      {
         m = pm.retrieve(messageID);
         if (log.isTraceEnabled()) { log.trace("Retreived it from persistent storage:" + m); }
      }
      catch(Throwable t)
      {
         log.error("Persistence manager failed", t);
      }

      if (m == null)
      {
         return null;
      }
      
      //Put it in the memory cache and return a ref from there
      return super.reference((Message)m);
      
   }
   

   public void remove(MessageReference ref) throws Throwable
   {
      super.remove(ref);
      
      if (ref.isReliable())
      {      
         //if (log.isTraceEnabled()) { log.trace("Removing message ref from persistent store"); }
         //pm.remove((String)ref.getMessageID());
      }
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

  
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
