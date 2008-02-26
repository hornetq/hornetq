/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_CREATESESSION;

import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class ConnectionCreateSessionMessageCodec extends
      AbstractPacketCodec<ConnectionCreateSessionMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionCreateSessionMessageCodec()
   {
      super(CONN_CREATESESSION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(ConnectionCreateSessionMessage request, RemotingBuffer out) throws Exception
   {
      boolean xa = request.isXA();
      boolean autoCommitSends = request.isAutoCommitSends();
      boolean autoCommitAcks = request.isAutoCommitAcks();
      

      int bodyLength = 3;

      out.putInt(bodyLength);

      out.putBoolean(xa);
      out.putBoolean(autoCommitSends);
      out.putBoolean(autoCommitAcks);
      
   }

   @Override
   protected ConnectionCreateSessionMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      boolean xa = in.getBoolean();
      boolean autoCommitSends = in.getBoolean();
      boolean autoCommitAcks = in.getBoolean();

      return new ConnectionCreateSessionMessage(xa, autoCommitSends, autoCommitAcks);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
