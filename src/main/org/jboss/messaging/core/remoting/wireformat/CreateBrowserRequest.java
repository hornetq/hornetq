/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEBROWSER;

import org.jboss.messaging.core.Destination;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class CreateBrowserRequest extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Destination destination;
   private final String selector;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateBrowserRequest(Destination destination, String selector)
   {
      super(REQ_CREATEBROWSER);

      assert destination != null;

      this.destination = destination;
      this.selector = selector;
   }

   // Public --------------------------------------------------------

   public Destination getDestination()
   {
      return destination;
   }

   public String getSelector()
   {
      return selector;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", destination=" + destination + ", selector="
            + selector + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
