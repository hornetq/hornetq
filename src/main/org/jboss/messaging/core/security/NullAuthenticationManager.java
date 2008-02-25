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
package org.jboss.messaging.core.security;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.jboss.security.RealmMapping;

/**
 * This is an implementation of AuthenticationManager and RealmMapping to use when we run embedded. The one we use when in jBoss
 * is not available. currently this does not have any functionality. A user can
 * provide their own implementation if security is needed
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class NullAuthenticationManager implements org.jboss.security.AuthenticationManager, RealmMapping
{
	public String getSecurityDomain()
	{
		return "messaging";
	}

	public boolean isValid(Principal principal, Object object)
	{
		return true;
	}

	public boolean isValid(Principal principal, Object object, Subject subject)
	{
		return true;
	}

	public Subject getActiveSubject()
	{
		return null;
	}

	public Principal getPrincipal(Principal principal)
	{
		return null;
	}

	public boolean doesUserHaveRole(Principal principal, Set set)
	{
		return true;
	}

	public Set getUserRoles(Principal principal)
	{
		return null;
	}

	public boolean isValid(javax.security.auth.message.MessageInfo messageInfo, Subject subject, String string)
	{
		return false;  //To change body of implemented methods use File | Settings | File Templates.
	}

	public Principal getTargetPrincipal(Principal principal, Map<String, Object> map)
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}
}
