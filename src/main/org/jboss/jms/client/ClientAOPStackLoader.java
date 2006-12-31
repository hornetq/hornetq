/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.jms.client.container.JmsClientAspectXMLLoader;

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
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

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

   // Attributes ----------------------------------------------------

   private static ClientAOPStackLoader instance;

   private boolean loaded;

   // Constructors --------------------------------------------------

   private ClientAOPStackLoader()
   {
      loaded = false;
   }

   // Public --------------------------------------------------------

   /**
    * @param delegate - either an instance of ClientClusteredConnectionFactoryDelegate or
    *        ClientConnectionFactoryDelegate.
    *
    * @throws Exception - if something goes wrong with downloading the AOP configuration from the
    *         server and installing it.
    */
   public synchronized void load(ClientAOPStackProvider delegate) throws Exception
   {
      if (loaded)
      {
         return;
      }

      byte[] clientAOPStack = delegate.getClientAOPStack();

      new JmsClientAspectXMLLoader().deployXML(clientAOPStack);

      loaded = true;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
