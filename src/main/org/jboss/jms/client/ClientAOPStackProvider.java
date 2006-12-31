/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

/**
 * An interface implemented by an object that can provide a client-side AOP stack configuration.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ClientAOPStackProvider
{
   /**
    * @return the AOP stack as byte[]. Use JmsClientAspectXMLLoader to deploy it on the client.
    */
   byte[] getClientAOPStack();
}
