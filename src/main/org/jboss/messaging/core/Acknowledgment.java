/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.io.Serializable;
import java.util.Set;
import java.util.Collections;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Acknowledgment
{
   // Constants -----------------------------------------------------

   public static final Acknowledgment ACK = new Acknowledgment()
   {
      public Serializable getReceiverID()
      {
         return null;
      }
      public boolean isPositive()
      {
         return true;
      }
      public boolean isNegative()
      {
         return false;
      }
   };

   public static final Acknowledgment NACK = new Acknowledgment()
   {
      public Serializable getReceiverID()
      {
         return null;
      }
      public boolean isPositive()
      {
         return false;
      }
      public boolean isNegative()
      {
         return true;
      }
   };

   public static final Set ACKSet = Collections.singleton(ACK);
   public static final Set NACKSet = Collections.singleton(NACK);

   // Public  -------------------------------------------------

   Serializable getReceiverID();

   boolean isPositive();

   boolean isNegative();
}
