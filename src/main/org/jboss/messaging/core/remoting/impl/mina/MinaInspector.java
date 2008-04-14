/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.apache.mina.filter.reqres.ResponseType.WHOLE;

import org.apache.mina.filter.reqres.ResponseInspector;
import org.apache.mina.filter.reqres.ResponseType;
import org.jboss.messaging.core.remoting.Packet;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class MinaInspector implements ResponseInspector
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ResponseInspector implementation ------------------------------

   public Object getRequestId(Object message)
   {
      if (!(message instanceof Packet))
      {
         return null;
      }
      Packet packet = (Packet) message;
      long id = packet.getCorrelationID();
      
      if (id == -1)
      {
      	return null;
      }
      else
      {
      	return id;
      }
   }

   public ResponseType getResponseType(Object message)
   {
      if (!(message instanceof Packet))
      {
         return null;
      }

      return WHOLE;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}