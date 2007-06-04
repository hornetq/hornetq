/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.jms.client.container.JmsClientAspectXMLLoader;
import org.jboss.jms.delegate.ConnectionFactoryEndpoint;

/**
 * A static singleton that insures the client-side AOP stack is loaded.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientAOPStackLoader
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   public static ClientAOPStackLoader getInstance()
   {
      synchronized(ClientAOPStackLoader.class)
      {
         if (instance == null)
         {
            instance = new ClientAOPStackLoader();
         }
         return instance;
      }
   }

   // Attributes -----------------------------------------------------------------------------------

   private static ClientAOPStackLoader instance;

   private boolean loaded;

   // Constructors ---------------------------------------------------------------------------------

   private ClientAOPStackLoader()
   {
      loaded = false;
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * @param delegate - either an instance of ClientClusteredConnectionFactoryDelegate or
    *        ClientConnectionFactoryDelegate.
    *
    * @throws Exception - if something goes wrong with downloading the AOP configuration from the
    *         server and installing it.
    */
   public synchronized void load(ConnectionFactoryEndpoint delegate) throws Exception
   {
      if (loaded)
      {
         return;
      }

      ClassLoader savedLoader = Thread.currentThread().getContextClassLoader();

      try
      {
         // This was done because of some weird behavior of AOP & classLoading
         // http://jira.jboss.org/jira/browse/JBMESSAGING-980
         Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

         byte[] clientAOPStack = delegate.getClientAOPStack();

         new JmsClientAspectXMLLoader().deployXML(clientAOPStack);

         loaded = true;
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(savedLoader);
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
