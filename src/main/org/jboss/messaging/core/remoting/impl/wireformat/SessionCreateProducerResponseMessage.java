/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEPRODUCER_RESP;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateProducerResponseMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String producerID;
   
   private final int initialTokens;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerResponseMessage(final String producerID, final int initialTokens)
   {
      super(SESS_CREATEPRODUCER_RESP);

      this.producerID = producerID;
      
      this.initialTokens = initialTokens;
   }

   // Public --------------------------------------------------------

   public String getProducerID()
   {
      return producerID;
   }
   
   public int getInitialTokens()
   {
   	return initialTokens;
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", producerID=" + producerID);
      buf.append(", initialTokens=" + initialTokens);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
