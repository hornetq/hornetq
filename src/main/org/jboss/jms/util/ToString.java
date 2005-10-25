/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.jms.util;

import javax.jms.DeliveryMode;
import javax.jms.Session;

/**
 * A collection of static string convertors.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ToString
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static String deliveryMode(int m)
   {
      if (m == DeliveryMode.NON_PERSISTENT)
      {
         return "NON_PERSISTENT";
      }
      if (m == DeliveryMode.PERSISTENT)
      {
         return "PERSISTENT";
      }
      return "UNKNOWN";
   }

   public static String acknowledgmentMode(int ack)
   {
      if (ack == Session.AUTO_ACKNOWLEDGE)
      {
         return "AUTO_ACKNOWLEDGE";
      }
      if (ack == Session.CLIENT_ACKNOWLEDGE)
      {
         return "CLIENT_ACKNOWLEDGE";
      }
      if (ack == Session.DUPS_OK_ACKNOWLEDGE)
      {
         return "DUPS_OK_ACKNOWLEDGE";
      }
      return "UNKNOWN";
   }


   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
