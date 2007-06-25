/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2006, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.impl.tx;

import javax.transaction.xa.Xid;

/**
 * A value object for prepared transaction representation
 *
 * @author <a href="mailto:Konda.Madhu@uk.mizuho-sc.com">Madhu Konda</a>
 */
public class PreparedTxInfo
{

   // Attributes --------------------------------------------------------------

	private long txId;

	private Xid xid = null;


   // Constructors ------------------------------------------------------------

	public PreparedTxInfo(long txId, Xid xid) {
		setTxId(txId);
		setXid(xid);
	}


   // Public ------------------------------------------------------------------

	public long getTxId() {
		return txId;
	}

	public void setTxId(long txId) {
		this.txId = txId;
	}

	public Xid getXid() {
		return xid;
	}

	public void setXid(Xid xid) {
		this.xid = xid;
	}


   // Object overrides --------------------------------------------------------

	public String toString()
	{
		return "Tx Id: "+getTxId()+" Xid: "+getXid();
	}
}