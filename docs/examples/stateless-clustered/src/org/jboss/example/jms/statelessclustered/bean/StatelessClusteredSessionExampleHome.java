/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.statelessclustered.bean;

import java.rmi.RemoteException;

import javax.ejb.CreateException;
import javax.ejb.EJBHome;
/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>

 * $Id$
 */

public interface StatelessClusteredSessionExampleHome extends EJBHome
{
   public StatelessClusteredSessionExample create() throws RemoteException, CreateException;
}


