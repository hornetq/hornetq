/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.local;

import org.jboss.messaging.core.TransactionalChannelSupport;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.MessageStore;

import javax.transaction.TransactionManager;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class Queue extends TransactionalChannelSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public Queue(String name, MessageStore ms)
   {
      this(name, ms, null, null);
   }

   public Queue(String name, MessageStore ms, TransactionManager tm)
   {
      this(name, ms, null, tm);
   }

   public Queue(String name, MessageStore ms, PersistenceManager pm)
   {
      this(name, ms, pm, null);
   }

   public Queue(String name, MessageStore ms, PersistenceManager pm, TransactionManager tm)
   {
      super(name, ms, pm, tm);
      router = new PointToPointRouter();
   }

   // Channel implementation ----------------------------------------

   public boolean isStoringUndeliverableMessages()
   {
      return true;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "CoreQueue[" + getChannelID() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
