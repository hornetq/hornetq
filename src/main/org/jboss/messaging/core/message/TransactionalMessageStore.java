/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.message;

import javax.transaction.TransactionManager;
import java.io.Serializable;

/**
 * TODO incomplete implementation
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TransactionalMessageStore extends MessageStoreSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected TransactionManager tm;

   // Constructors --------------------------------------------------

   public TransactionalMessageStore(Serializable storeID, TransactionManager tm)
   {
      super(storeID);
      this.tm = tm;
   }

   // MessageStore implementation -----------------------------------

   public boolean isReliable()
   {
      return false;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
