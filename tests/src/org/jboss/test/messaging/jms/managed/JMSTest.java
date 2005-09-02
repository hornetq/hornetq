/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.managed;

import java.rmi.RemoteException;
import java.rmi.Remote;
import javax.ejb.*;
  
public interface JMSTest extends EJBObject, Remote
{
  
  public void test1() throws RemoteException;
}
  

