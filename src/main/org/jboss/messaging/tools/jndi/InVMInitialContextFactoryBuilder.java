/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.tools.jndi;

import javax.naming.spi.InitialContextFactoryBuilder;
import javax.naming.spi.InitialContextFactory;
import javax.naming.NamingException;
import java.util.Hashtable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class InVMInitialContextFactoryBuilder implements InitialContextFactoryBuilder
{
   public InitialContextFactory createInitialContextFactory(Hashtable environment)
         throws NamingException
   {
      return new InVMInitialContextFactory();
   }
}
