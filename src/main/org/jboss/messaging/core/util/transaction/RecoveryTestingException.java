/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util.transaction;

/**
 * This exception is used by the recovery testing framework to force the TM to abort at
 * certain points so that recovery logging can be tested.
 *
 * @author <a href="mailto:bill@jboss.org">Bill Burke</a>
 * @version $Revision$
 */
class RecoveryTestingException extends RuntimeException
{
   public RecoveryTestingException()
   {
   }

   public RecoveryTestingException(String message)
   {
      super(message);
   }

   public RecoveryTestingException(String message, Throwable cause)
   {
      super(message, cause);
   }

   public RecoveryTestingException(Throwable cause)
   {
      super(cause);
   }
}
