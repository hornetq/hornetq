/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;

import javax.jms.Message;
import javax.jms.Destination;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerProducerDelegate implements ProducerDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerProducerDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   protected Receiver destination;
   /** I need this to set up the JMSDestination header on outgoing messages */
   protected Destination jmsDestination;
   protected ServerSessionDelegate sessionEndpoint;

   // Constructors --------------------------------------------------

   public ServerProducerDelegate(String id, Receiver destination,
                                 Destination jmsDestination, ServerSessionDelegate parent)
   {
      this.id = id;
      this.destination = destination;
      this.jmsDestination = jmsDestination;
      sessionEndpoint = parent;
   }

   // ProducerDelegate implementation ------------------------

   public void send(Message m)
   {
      if (log.isTraceEnabled()) { log.trace("sending message " + m + " to the core"); }

      try
      {
         // TODO JMS1.1 specs 3.4.3. If the client can live without a messageID, do not set a new messageID on the message
         m.setJMSMessageID(generateMessageID());
         m.setJMSDestination(jmsDestination);


         boolean acked = destination.handle((Routable)m);

         if (!acked)
         {
            log.debug("The message was not acknowledged");
         }
      }
      catch(Throwable t)
      {
         log.error("Message handling failure", t);
      }

   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   protected String generateMessageID()
   {
      StringBuffer sb = new StringBuffer("ID:");
      sb.append(new GUID().toString());
      return sb.toString();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
