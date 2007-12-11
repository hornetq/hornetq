/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEDESTINATION;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class CreateDestinationRequest extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String name;
   private final boolean isQueue;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateDestinationRequest(String name, boolean isQueue)
   {
      super(REQ_CREATEDESTINATION);

      assert name != null;
      assert name.length() > 0;

      this.name = name;
      this.isQueue = isQueue;
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }
   
   public boolean isQueue()
   {
      return isQueue;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", name=" + name + ", isQueue=" + isQueue + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
