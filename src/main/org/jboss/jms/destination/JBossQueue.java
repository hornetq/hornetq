/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.destination;

import javax.jms.Queue;
import javax.jms.JMSException;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossQueue extends JBossDestination implements Queue
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 4121129234371655479L;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public JBossQueue(String name)
   {
      super(name);
   }

   // JBossDestination overrides ------------------------------------

   public boolean isTopic()
   {
      return false;
   }

   public boolean isQueue()
   {
      return true;
   }

   // Queue implementation ------------------------------------------

   public String getQueueName() throws JMSException
   {
      return getName();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
