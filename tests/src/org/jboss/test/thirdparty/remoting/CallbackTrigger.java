/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CallbackTrigger implements Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2887545875458754L;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private String payload;

   // Constructors ---------------------------------------------------------------------------------

   public CallbackTrigger(String payload)
   {
      this.payload = payload;
   }

   // Public ---------------------------------------------------------------------------------------

   public String getPayload()
   {
      return payload;
   }

   public String toString()
   {
      return "CallbackTrigger[" + payload + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
