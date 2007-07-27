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
package org.jboss.test.messaging.tools.container;

import org.jboss.jms.server.DestinationManager;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * RemoteTestServer.java,v 1.1 2006/02/21 08:25:33 timfox Exp
 */
public class RemoteTestServer extends LocalTestServer
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public RemoteTestServer(int serverIndex)
   {
      super(serverIndex);
   }

   // Server implementation ------------------------------------------------------------------------

   /**
    * Only for in-VM use!
    */
   public MessageStore getMessageStore() throws Exception
   {
      throw new UnsupportedOperationException("This method shouldn't be invoked on a remote server");
   }

   /**
    * Only for in-VM use!
    */
   public DestinationManager getDestinationManager() throws Exception
   {
      throw new UnsupportedOperationException("This method shouldn't be invoked on a remote server");
   }

   /**
    * Only for in-VM use!
    */
   public PersistenceManager getPersistenceManager() throws Exception
   {
      throw new UnsupportedOperationException("This method shouldn't be invoked on a remote server");
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
