/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
   * by the @authors tag. See the copyright.txt in the distribution for a
   * full listing of individual contributors.
   *
   * This is free software; you can redistribute it and/or modify it
   * under the terms of the GNU Lesser General Public License as
   * published by the Free Software Foundation; either version 2.1 of
   * the License, or (at your option) any later version.
   *
   * This software is distributed in the hope that it will be useful,
   * but WITHOUT ANY WARRANTY; without even the implied warranty of
   * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   * Lesser General Public License for more details.
   *
   * You should have received a copy of the GNU Lesser General Public
   * License along with this software; if not, write to the Free
   * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
   * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
   */
package org.jboss.messaging.microcontainer;

import org.jboss.kernel.spi.dependency.KernelControllerContext;
import org.jboss.kernel.spi.dependency.KernelControllerContextAware;
import org.jboss.tm.TransactionManagerLocator;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

/**
 * This is a layer that is used for injecting services into other objects. depending on the configuration we are running
 * we can get these from more than one place or even inject them.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServiceLocator implements KernelControllerContextAware
{
    private ObjectName multiplexer;
    private TransactionManager tm;
    private DataSource dataSource;
    private KernelControllerContext kernelControllerContext;

    public void setKernelControllerContext(KernelControllerContext kernelControllerContext) throws Exception
    {
        this.kernelControllerContext = kernelControllerContext;
    }

    public void unsetKernelControllerContext(KernelControllerContext kernelControllerContext) throws Exception
    {
        kernelControllerContext = null;
    }

    public TransactionManager getTransactionManager() throws MalformedObjectNameException
    {
        if(tm == null)
        {
            tm = TransactionManagerLocator.locateTransactionManager();
        }
        return tm;
    }

    public void setTransactionManager(TransactionManager transactionManager)
    {
        this.tm = transactionManager;
    }

    public DataSource getDataSource() throws Exception
    {
        if(dataSource == null)
        {
           InitialContext ic = new InitialContext();
           //try in the initial context, if its not there use the one that has been injected
           try
           {
              dataSource = (DataSource)ic.lookup("java:/DefaultDS");
           }
           catch (Exception e)
           {
              dataSource = (DataSource) kernelControllerContext.getController().getInstalledContext("jboss.jca:name=DefaultDS,service=DataSourceBinding").getTarget();
           }
        }
        return dataSource;
    }

    public void setDataSource(DataSource datasource)
    {
        this.dataSource = datasource;
    }



    public ObjectName getMultiplexer()
    {
        return multiplexer;
    }

    public void setMultiplexer(ObjectName multiplexer)
    {
        this.multiplexer = multiplexer;
    }
                /*if(mBeanServer != null)
            {
                TransactionManagerServiceMBean tms =
                    (TransactionManagerServiceMBean) MBeanServerInvocationHandler.
                            newProxyInstance(mBeanServer, new ObjectName("jboss:service=TransactionManager"), TransactionManagerServiceMBean.class, false);

                tm = tms.getTransactionManager();
            }
            else
            {
                tm = (TransactionManager) kernelControllerContext.getKernel().getRegistry().getEntry("TransactionManager").getTarget();
            }*/
}
