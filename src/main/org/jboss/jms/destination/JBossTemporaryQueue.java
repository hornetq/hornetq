/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.destination;

import javax.jms.TemporaryQueue;
import javax.jms.JMSException;

import org.jboss.jms.client.SessionDelegate;

/**
 * A temporary queue
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossTemporaryQueue
   extends JBossQueue
   implements TemporaryQueue
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The session */
   private SessionDelegate delegate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Construct a new temporary queue
    * 
    * @param delegate the session
    * @param name the name
    * @throws JMSException for any error
    */
   public JBossTemporaryQueue(SessionDelegate delegate, String name)
      throws JMSException
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
