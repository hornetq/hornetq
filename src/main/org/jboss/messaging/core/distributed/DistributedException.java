/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

/**
 * A wrapper for the exceptions thrown by the distributed layer (JGroups).
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributedException extends Exception
{
   // Constructors --------------------------------------------------

   public DistributedException() {
      super();
   }

   public DistributedException(String message) {
      super(message);
   }

   public DistributedException(String message, Throwable cause) {
      super(message, cause);
   }

   public DistributedException(Throwable cause) {
      super(cause);
   }
}
