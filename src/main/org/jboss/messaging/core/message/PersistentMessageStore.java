/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.message;

import java.io.Serializable;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.Routable;

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

   public PersistentMessageStore(Serializable storeID, PersistenceManager pm)
   {
      super(storeID);
      this.pm = pm;
   }

   // MessageStore overrides ----------------------------------------

   public boolean isReliable()
   {
      return true;
   }
   
   public MessageReference reference(Routable r)
   {
      if (r.isReference())
      {
         if (log.isTraceEnabled()) { log.trace("Routable is already a reference"); }
         return (MessageReference)r;
      }
      
      MessageReference ref = super.getReference(r.getMessageID());
      
      if (ref != null)
      {        
         if (log.isTraceEnabled()) { log.trace("Retrieved it from memory cache"); }
         return ref;
      }
      
      /*
      if (r.isRecoverable())
      {
         //Maybe it's on disk already?
         Message m = retrieveMessage(r.getMessageID());
         if (m != null)
         {
            ref = super.createReference(m);
         }
      }
      */
           
      if (ref == null)
      {
         //Message doesn't exist either in memory on disc
         if (r.isReliable())
         {
            try
            {
               pm.store((Message)r);
               if (log.isTraceEnabled()) { log.trace("Store message " + r); }
            }
            catch (Exception e)
            {
               log.error("Failed to store message", e);
               return null;
            }
         }
         ref= super.createReference(r);
      }
      return ref;
   }

   
   public MessageReference getReference(Serializable messageID)
   {
      if (log.isTraceEnabled()) { log.trace("Getting message ref for message ID: " + messageID);}
      
      //Try and get the reference from the in memory cache first
      MessageReference ref = super.getReference(messageID);
      
      if (ref != null)
      {        
         if (log.isTraceEnabled()) { log.trace("Retrieved it from memory cache"); }
         return ref;
      }

      //Try and retrieve it from persistent storage
      Message m = retrieveMessage(messageID);

      if (m != null)
      {
         //Put it in the memory cache and return a ref from there
         ref = super.createReference((Message)m);
      }
      
      return ref;
      
   }
   
   protected Message retrieveMessage(Serializable messageID)
   {
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
      return m;
   }
   

  


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

  
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
