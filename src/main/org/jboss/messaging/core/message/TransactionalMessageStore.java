/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.message;

import java.io.Serializable;

/**
 * TODO incomplete implementation
 * 
 * What's the point of this class?? 
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TransactionalMessageStore extends UnreliableMessageStore
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------  

   // Constructors --------------------------------------------------

   public TransactionalMessageStore(Serializable storeID)
   {
      super(storeID);
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
