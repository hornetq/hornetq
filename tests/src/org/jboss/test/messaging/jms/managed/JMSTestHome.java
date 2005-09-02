
package org.jboss.test.messaging.jms.managed;

import java.rmi.RemoteException;

import javax.ejb.CreateException;
import javax.ejb.EJBHome;


public interface JMSTestHome extends EJBHome
{
   JMSTest create() throws RemoteException, CreateException;
}
