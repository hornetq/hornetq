/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.destination;

import javax.jms.JMSException;

/**
 * A temporary destination
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface JBossTemporaryDestination
{
   // Constants -----------------------------------------------------

   /** Temporary queue */
   public static final int TEMPORARY_QUEUE = 1;

   /** Temporary topic */
   public static final int TEMPORARY_TOPIC = 2;

   // Public --------------------------------------------------------

   /**
    * Delete the temporary destination
    * 
    * @throws JMSExeption for any error
    */   
   void delete() throws JMSException;

   // Inner Classes --------------------------------------------------

}
