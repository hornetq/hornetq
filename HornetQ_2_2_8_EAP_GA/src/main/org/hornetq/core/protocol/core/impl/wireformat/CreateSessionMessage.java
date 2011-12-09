/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateSessionMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String name;

   private long sessionChannelID;

   private int version;

   private String username;

   private String password;

   private int minLargeMessageSize;

   private boolean xa;

   private boolean autoCommitSends;

   private boolean autoCommitAcks;

   private boolean preAcknowledge;

   private int windowSize;
   
   private String defaultAddress;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateSessionMessage(final String name,
                               final long sessionChannelID,
                               final int version,
                               final String username,
                               final String password,
                               final int minLargeMessageSize,
                               final boolean xa,
                               final boolean autoCommitSends,
                               final boolean autoCommitAcks,
                               final boolean preAcknowledge,
                               final int windowSize,
                               final String defaultAddress)
   {
      super(PacketImpl.CREATESESSION);

      this.name = name;

      this.sessionChannelID = sessionChannelID;

      this.version = version;

      this.username = username;

      this.password = password;

      this.minLargeMessageSize = minLargeMessageSize;

      this.xa = xa;

      this.autoCommitSends = autoCommitSends;

      this.autoCommitAcks = autoCommitAcks;

      this.windowSize = windowSize;

      this.preAcknowledge = preAcknowledge;
      
      this.defaultAddress = defaultAddress;
   }

   public CreateSessionMessage()
   {
      super(PacketImpl.CREATESESSION);
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }

   public long getSessionChannelID()
   {
      return sessionChannelID;
   }

   public int getVersion()
   {
      return version;
   }

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }

   public boolean isXA()
   {
      return xa;
   }

   public boolean isAutoCommitSends()
   {
      return autoCommitSends;
   }

   public boolean isAutoCommitAcks()
   {
      return autoCommitAcks;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public int getWindowSize()
   {
      return windowSize;
   }
   
   public String getDefaultAddress()
   {
      return defaultAddress;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeString(name);
      buffer.writeLong(sessionChannelID);
      buffer.writeInt(version);
      buffer.writeNullableString(username);
      buffer.writeNullableString(password);
      buffer.writeInt(minLargeMessageSize);
      buffer.writeBoolean(xa);
      buffer.writeBoolean(autoCommitSends);
      buffer.writeBoolean(autoCommitAcks);
      buffer.writeInt(windowSize);
      buffer.writeBoolean(preAcknowledge);
      buffer.writeNullableString(defaultAddress);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      name = buffer.readString();
      sessionChannelID = buffer.readLong();
      version = buffer.readInt();
      username = buffer.readNullableString();
      password = buffer.readNullableString();
      minLargeMessageSize = buffer.readInt();
      xa = buffer.readBoolean();
      autoCommitSends = buffer.readBoolean();
      autoCommitAcks = buffer.readBoolean();
      windowSize = buffer.readInt();
      preAcknowledge = buffer.readBoolean();
      defaultAddress = buffer.readNullableString();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof CreateSessionMessage == false)
      {
         return false;
      }

      CreateSessionMessage r = (CreateSessionMessage)other;

      boolean matches = super.equals(other) && name.equals(r.name) &&
                        sessionChannelID == r.sessionChannelID &&
                        version == r.version &&
                        xa == r.xa &&
                        autoCommitSends == r.autoCommitSends &&
                        autoCommitAcks == r.autoCommitAcks &&
                        (username == null ? r.username == null : username.equals(r.username)) &&
                        (password == null ? r.password == null : password.equals(r.password)) &&
                        (defaultAddress == null ? r.defaultAddress == null : defaultAddress.equals(r.defaultAddress));
                        

      return matches;
   }

   @Override
   public final boolean isRequiresConfirmations()
   {
      return false;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
