/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.message;

import java.io.Serializable;

/**
 * Maintains the identity of a message relative to a specific MessageStore.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class StorageIdentifier
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   public final Serializable messageID;
   public final Serializable storeID;

   // Constructors --------------------------------------------------

   public StorageIdentifier(Serializable messageID, Serializable storeID)
   {
      this.messageID = messageID;
      this.storeID = storeID;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
