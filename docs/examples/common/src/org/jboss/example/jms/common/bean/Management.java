/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.common.bean;

import java.rmi.RemoteException;

import javax.ejb.EJBObject;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1766 $</tt>

 * $Id: StatelessSessionExample.java 1766 2006-12-11 22:29:27Z ovidiu.feodorov@jboss.com $
 */

public interface Management extends EJBObject
{
   /**
    * It kills the VM running the node instance. Needed by the failover tests.
    */
   public void killAS() throws Exception, RemoteException;
}
