/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting.util;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class OnewayCallbackTrigger implements Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2887545875458754L;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private String payload;
   private long[] triggerTimes;

   // Constructors ---------------------------------------------------------------------------------

   public OnewayCallbackTrigger(String payload)
   {
      this(payload, new long[] {0});
   }

   public OnewayCallbackTrigger(String payload, long[] triggerTimes)
   {
      this.payload = payload;
      this.triggerTimes = triggerTimes;
   }

   // Public ---------------------------------------------------------------------------------------

   public String getPayload()
   {
      return payload;
   }

   public long[] getTriggerTimes()
   {
      return triggerTimes;
   }

   public String toString()
   {
      return "OnewayCallbackTrigger[" + payload + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
