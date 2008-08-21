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

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

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
   
   private int version;
   
   private String username;
   
   private String password;
   
   private boolean xa;
   
   private boolean autoCommitSends;
   
   private boolean autoCommitAcks;
   
   private long commandResponseTargetID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateSessionMessage(final String name, final int version, final String username, final String password,
                               final boolean xa, final boolean autoCommitSends,
                               final boolean autoCommitAcks, final long commandResponseTargetID)
   {
      super(CREATESESSION);
      
      this.name = name;
      
      this.version = version;

      this.username = username;
      
      this.password = password;
      
      this.xa = xa;
      
      this.autoCommitSends = autoCommitSends;
      
      this.autoCommitAcks = autoCommitAcks;
      
      this.commandResponseTargetID = commandResponseTargetID;
   }
   
   public CreateSessionMessage()
   {
      super(CREATESESSION);
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
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
      return this.autoCommitSends;
   }
   
   public boolean isAutoCommitAcks()
   {
      return this.autoCommitAcks;
   }
   
   public long getCommandResponseTargetID()
   {
      return commandResponseTargetID;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putString(name);
      buffer.putInt(version);
      buffer.putNullableString(username);
      buffer.putNullableString(password);
      buffer.putBoolean(xa);
      buffer.putBoolean(autoCommitSends);
      buffer.putBoolean(autoCommitAcks);
      buffer.putLong(commandResponseTargetID);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      name = buffer.getString();
      version = buffer.getInt();
      username = buffer.getNullableString();
      password = buffer.getNullableString();
      xa = buffer.getBoolean();
      autoCommitSends = buffer.getBoolean();
      autoCommitAcks = buffer.getBoolean();
      commandResponseTargetID = buffer.getLong();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof CreateSessionMessage == false)
      {
         return false;
      }
            
      CreateSessionMessage r = (CreateSessionMessage)other;
      
      boolean matches = super.equals(other) &&
                        this.name.equals(r.name) &&
                        this.version == r.version &&
                        this.xa == r.xa &&
                        this.autoCommitSends == r.autoCommitSends &&
                        this.autoCommitAcks == r.autoCommitAcks &&
                        this.commandResponseTargetID == r.commandResponseTargetID &&
                        (this.username == null ? r.username == null : this.username.equals(r.username)) &&
                        (this.password == null ? r.password == null : this.password.equals(r.password));
         
      return matches;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
