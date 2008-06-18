/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.core.version.impl.VersionImpl;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateConnectionResponse extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long connectionTargetID;

   private Version serverVersion;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConnectionResponse(final long connectionTargetID, final Version serverVersion)
   {
      super(CREATECONNECTION_RESP);

      this.connectionTargetID = connectionTargetID;
      this.serverVersion = serverVersion;
   }
   
   public CreateConnectionResponse()
   {
      super(CREATECONNECTION_RESP);
   }

   // Public --------------------------------------------------------

   public long getConnectionTargetID()
   {
      return connectionTargetID;
   }

   public Version getServerVersion()
   {
      return serverVersion;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(connectionTargetID);
      buffer.putNullableString(serverVersion.getVersionName());
      buffer.putInt(serverVersion.getMajorVersion());
      buffer.putInt(serverVersion.getMinorVersion());
      buffer.putInt(serverVersion.getMicroVersion());
      buffer.putInt(serverVersion.getIncrementingVersion());
      buffer.putNullableString(serverVersion.getVersionSuffix());
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      connectionTargetID = buffer.getLong();
      String versionName = buffer.getNullableString();
      int majorVersion = buffer.getInt();
      int minorVersion = buffer.getInt();
      int microVersion = buffer.getInt();
      int incrementingVersion = buffer.getInt();
      String versionSuffix = buffer.getNullableString();
      serverVersion =  new VersionImpl(versionName, majorVersion, minorVersion, microVersion, incrementingVersion, versionSuffix);
   }

   @Override
   public String toString()
   {
      return getParentString() + ", connectionID" + connectionTargetID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
