/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateConnectionRequest extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int version;
   private long remotingSessionID;
   private String username;
   private String password;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConnectionRequest(final int version,
         final long remotingSessionID, final String username, final String password)
   {
      super(CREATECONNECTION);

      this.version = version;
      this.remotingSessionID = remotingSessionID;
      this.username = username;
      this.password = password;
   }
   
   public CreateConnectionRequest()
   {
      super(CREATECONNECTION);
   }

   // Public --------------------------------------------------------

   public int getVersion()
   {
      return version;
   }

   public long getRemotingSessionID()
   {
      return remotingSessionID;
   }

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(version);
      buffer.putLong(remotingSessionID);
      buffer.putNullableString(username);
      buffer.putNullableString(password);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      version = buffer.getInt();
      remotingSessionID = buffer.getLong();
      username = buffer.getNullableString();
      password = buffer.getNullableString();
   }
   
   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", version=" + version);
      buf.append(", remotingSessionID=" + remotingSessionID);
      buf.append(", username=" + username);
      buf.append(", password=" + password);
      buf.append("]");
      return buf.toString();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
