/**
 *
 */
package org.hornetq.api.core;

/**
 * Security exception thrown when the cluster user fails authentication.
 */
public final class HornetQClusterSecurityException extends HornetQException
{
   private static final long serialVersionUID = -5890578849781297933L;

   public HornetQClusterSecurityException()
   {
      super(HornetQExceptionType.CLUSTER_SECURITY_EXCEPTION);
   }

   public HornetQClusterSecurityException(final String msg)
   {
      super(HornetQExceptionType.CLUSTER_SECURITY_EXCEPTION, msg);
   }
}
