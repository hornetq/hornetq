/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.jms.util;

import javax.jms.DeliveryMode;

/**
 * A collection of static string convertors.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a> $Id$
 * @version <tt>$Revision$</tt>
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

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
