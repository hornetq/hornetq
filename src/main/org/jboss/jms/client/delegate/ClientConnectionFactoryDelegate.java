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
package org.jboss.jms.client.delegate;

import javax.jms.JMSException;

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.messaging.util.Util;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

/**
 * The client-side ConnectionFactory delegate class.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientConnectionFactoryDelegate
   extends DelegateSupport implements ConnectionFactoryDelegate
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 2512460695662741413L;
   
   // Attributes ----------------------------------------------------
   
   protected String serverLocatorURI;
   
   protected Client client;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public ClientConnectionFactoryDelegate(String objectID, String serverLocatorURI)
   {
      super(objectID);
      
      this.serverLocatorURI = serverLocatorURI;
   }
   
   // ConnectionFactoryDelegateImplementation -----------------------
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ConnectionDelegate createConnectionDelegate(String username, String password)
   throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public byte[] getClientAOPConfig()
   {      
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   // Public --------------------------------------------------------
   
   public void init()
   {
      super.init();
      
      //Initialise the client
      try
      {
         InvokerLocator locator = new InvokerLocator(this.serverLocatorURI);
         
         client = new Client(locator);
         
      }
      catch (Exception e)
      {
         throw new RuntimeException("Failed to create client", e);  
      }
      
   }
   
   public String toString()
   {
      return "ConnectionFactoryDelegate[" + Util.guidToString(id) + "]";
   }
   
   // Protected -----------------------------------------------------
   
   protected Client getClient()
   {
      return client;
   }
   
   // Package Private -----------------------------------------------
      
   // Private -------------------------------------------------------
      
   // Inner Classes -------------------------------------------------
      
}
