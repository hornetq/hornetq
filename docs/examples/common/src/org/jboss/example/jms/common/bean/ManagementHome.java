/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.common.bean;

import java.rmi.RemoteException;

import javax.ejb.CreateException;
import javax.ejb.EJBHome;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 563 $</tt>

 * $Id: StatelessSessionExampleHome.java 563 2005-12-30 22:04:19Z ovidiu $
 */

public interface ManagementHome extends EJBHome
{
   public Management create() throws RemoteException, CreateException;
}
