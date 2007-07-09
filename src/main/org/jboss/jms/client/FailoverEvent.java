/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.util.EventObject;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class FailoverEvent extends EventObject
{
   // Constants ------------------------------------------------------------------------------------

	private static final long serialVersionUID = 8451706459791859231L;
	
	public static final int FAILURE_DETECTED = 10;
   public static final int FAILOVER_STARTED = 20;
   public static final int FAILOVER_COMPLETED = 30;
   /** Failover was completed in parallel by another thread */
   public static final int FAILOVER_ALREADY_COMPLETED = 40;
   public static final int FAILOVER_FAILED = 100;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private int type;

   // Constructors ---------------------------------------------------------------------------------

   public FailoverEvent(int type, Object source)
   {
      super(source);
      checkType(type);
      this.type = type;
   }

   // Public ---------------------------------------------------------------------------------------

   public int getType()
   {
      return type;
   }

   public String toString()
   {
      return
         type == FAILURE_DETECTED ? "FAILURE_DETECTED" :
            type == FAILOVER_STARTED ? "FAILOVER_STARTED" :
               type == FAILOVER_COMPLETED ? "FAILOVER_COMPLETED" :
               	type == FAILOVER_ALREADY_COMPLETED ? "FAILOVER_ALREADY_COMPLETED" :
                     type == FAILOVER_FAILED ? "FAILOVER_FAILED" : "UNKNOWN_FAILOVER_EVENT";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void checkType(int type) throws IllegalArgumentException
   {
      if (type != FAILURE_DETECTED &&
         type != FAILOVER_STARTED &&
         type != FAILOVER_COMPLETED &&
         type != FAILOVER_FAILED &&
         type != FAILOVER_ALREADY_COMPLETED)
      {
         throw new IllegalArgumentException("Illegal failover event type: " + type);
      }
   }

   // Inner classes --------------------------------------------------------------------------------
}
