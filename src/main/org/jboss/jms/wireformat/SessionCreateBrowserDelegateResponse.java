/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.wireformat;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.jms.client.delegate.ClientBrowserDelegate;

/**
 * 
 * A SessionCreateBrowserDelegateResponse
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class SessionCreateBrowserDelegateResponse extends ResponseSupport
{
   private ClientBrowserDelegate del;
   
   public SessionCreateBrowserDelegateResponse()
   {      
   }
   
   public SessionCreateBrowserDelegateResponse(ClientBrowserDelegate del)
   {
      super(PacketSupport.RESP_SESSION_CREATEBROWSERDELEGATE);
      
      this.del = del;
   }

   public Object getResponse()
   {
      return del;
   }
   
   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      
      del.write(os);
      
      os.flush();
   }
   
   public void read(DataInputStream is) throws Exception
   {
      del = new ClientBrowserDelegate();
      
      del.read(is);
   }

}

