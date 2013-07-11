/**
 *
 */
package org.hornetq.utils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public final class ConfirmationWindowWarning
{
   public final boolean disabled;
   public final AtomicBoolean warningIssued;

   /**
    *
    */
   public ConfirmationWindowWarning(boolean disabled)
   {
      this.disabled = disabled;
      warningIssued = new AtomicBoolean(false);
   }
}
