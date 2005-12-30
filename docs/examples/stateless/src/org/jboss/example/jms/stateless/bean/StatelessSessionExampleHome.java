/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.stateless.bean;

import javax.ejb.EJBHome;
import java.rmi.RemoteException;
import javax.ejb.CreateException;
/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>

 * $Id$
 */

public interface StatelessSessionExampleHome extends EJBHome
{
   public StatelessSessionExample create() throws RemoteException, CreateException;
}


