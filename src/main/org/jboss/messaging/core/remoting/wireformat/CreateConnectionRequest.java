/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONNECTION;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateConnectionRequest extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final byte version;
   private final String remotingSessionID;
   private final String clientVMID;
   private final String username;
   private final String password;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConnectionRequest(byte version,
         String remotingSessionID, String clientVMID, String username, String password)
   {
      super(REQ_CREATECONNECTION);

      assertValidID(remotingSessionID);
      assertValidID(clientVMID);

      this.version = version;
      this.remotingSessionID = remotingSessionID;
      this.clientVMID = clientVMID;
      this.username = username;
      this.password = password;
   }

   // Public --------------------------------------------------------

   public byte getVersion()
   {
      return version;
   }

   public String getRemotingSessionID()
   {
      return remotingSessionID;
   }
   
   public String getClientVMID()
   {
      return clientVMID;
   }

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", version=" + version);
      buf.append(", remotingSessionID=" + remotingSessionID);
      buf.append(", clientVMID=" + clientVMID);
      buf.append(", username=" + username);
      buf.append(", password=" + password);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
