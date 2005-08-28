/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.message;

import org.jboss.messaging.core.Message;
import org.jboss.jms.message.JBossMessage;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a> 
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class Factory
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   public static Message createMessage(Serializable messageID,
                                       boolean reliable, 
                                       long expiration, 
                                       long timestamp,
                                       Serializable body)
   {
      return new JBossMessage((String)messageID, reliable, expiration, timestamp, body);
   }
   
   public static Message createMessage(Serializable messageID)
   {
      return createMessage(messageID, false, 0, 0, null);
   }
   
   public static Message createMessage(Serializable messageID,
                                       boolean reliable, 
                                       Serializable body)
   {
      return createMessage(messageID, reliable, 0, 0, body);
   }


   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
