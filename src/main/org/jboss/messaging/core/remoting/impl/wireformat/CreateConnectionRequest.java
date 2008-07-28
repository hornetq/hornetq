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

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateConnectionRequest extends PacketImpl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(CreateConnectionRequest.class);

   
   // Attributes ----------------------------------------------------

   private int version;
   private String username;
   private String password;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConnectionRequest(final int version, final String username, final String password)
   {
      super(CREATECONNECTION);

      this.version = version;
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
      buffer.putNullableString(username);
      buffer.putNullableString(password);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      version = buffer.getInt();
      username = buffer.getNullableString();
      password = buffer.getNullableString();
   }
   
   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", version=" + version);
      buf.append(", username=" + username);
      buf.append(", password=" + password);
      buf.append("]");
      return buf.toString();
   }

   public boolean equals(Object other)
   {
      if (other instanceof CreateConnectionRequest == false)
      {
         return false;
      }
            
      CreateConnectionRequest r = (CreateConnectionRequest)other;
      
      boolean matches = super.equals(other) &&
                        this.version == r.version &&                     
                        this.username == null ? r.username == null : this.username.equals(r.username) &&
                        this.password == null ? r.password == null : this.password.equals(r.password);
      
      return matches;
   }
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
