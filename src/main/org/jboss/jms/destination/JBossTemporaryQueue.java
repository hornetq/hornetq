/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.destination;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

import org.jboss.jms.client.ConnectionDelegate;

/**
 * A temporary queue
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossTemporaryQueue
   extends JBossQueue
   implements TemporaryQueue, JBossTemporaryDestination
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The connection */
   private ConnectionDelegate delegate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Construct a new temporary queue
    * 
    * @param delegate the connection
    * @param name the name
    */
   public JBossTemporaryQueue(ConnectionDelegate delegate, String name)
   {
      super(name);
      this.delegate = delegate;
   }

   // Public --------------------------------------------------------

   // TemporaryQueue implementation ---------------------------------

   public void delete()
      throws JMSException
   {
      delegate.deleteTempDestination(this);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
