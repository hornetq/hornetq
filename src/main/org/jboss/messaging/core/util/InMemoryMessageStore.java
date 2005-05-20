/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Message;

import java.io.Serializable;
import java.util.Map;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class InMemoryMessageStore implements MessageStore
{
   // Constants -----------------------------------------------------


   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable storeID;
   protected Map map;

   // Constructors --------------------------------------------------

   public InMemoryMessageStore(Serializable storeID)
   {
      this.storeID = storeID;
   }

   // MessageStore implementation ----------------------------------

   public Serializable getStoreID()
   {
      return storeID;
   }

   public MessageReference store(Message m) throws Throwable
   {
      return null;
   }

   public Message retrieve(MessageReference r)
   {
      return null;
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
