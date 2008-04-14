/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.exception.MessagingException;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class RemotingException extends MessagingException
{

   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -6849945921631932738L;

   // Attributes ----------------------------------------------------

   private long sessionID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public RemotingException(int code, String message, long sessionID)
   {
      super(code, message);
      this.sessionID = sessionID;
   }

   // Public --------------------------------------------------------

   public long getSessionID()
   {
      return sessionID;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
