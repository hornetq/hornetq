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
package org.jboss.jms.server.security;

import org.jboss.logging.Logger;
import org.jboss.security.SimplePrincipal;

import java.util.HashSet;
import java.util.Set;

/**
 * SecurityMetadata.java
 * <p/>
 * <p/>
 * Created: Tue Feb 26 15:02:29 2002
 *
 * @author Peter
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */

public class SecurityMetadata
{
    static Role DEFAULT_ROLE = new Role("guest", true, true, true);

    HashSet<Role> roles = new HashSet<Role>();
    HashSet<SimplePrincipal> read = new HashSet<SimplePrincipal>();
    HashSet<SimplePrincipal> write = new HashSet<SimplePrincipal>();
    HashSet<SimplePrincipal> create = new HashSet<SimplePrincipal>();
    static Logger log = Logger.getLogger(SecurityMetadata.class);

    /**
     * create with default roles
     */
    public SecurityMetadata()
    {
        addRole(DEFAULT_ROLE);
    }

    /**
     * create with roles provided
     * @param roles
     * @throws Exception
     */
    public SecurityMetadata(HashSet<Role> roles)
    {
        setRoles(roles);
    }

    public void addRole(String name, boolean read, boolean write, boolean create)
    {
        Role r = new Role(name, read, write, create);
        addRole(r);
    }

    public void addRole(Role r)
    {
        if (log.isTraceEnabled())
            log.trace("Adding role: " + r.toString());

        roles.add(r);
        SimplePrincipal p = new SimplePrincipal(r.name);
        if (r.isRead())
            read.add(p);
        if (r.isWrite())
            write.add(p);
        if (r.isCreate())
            create.add(p);
    }

    public Set<SimplePrincipal> getReadPrincipals()
    {
        return read;
    }

    public Set<SimplePrincipal> getWritePrincipals()
    {
        return write;
    }

    public Set<SimplePrincipal> getCreatePrincipals()
    {
        return create;
    }


    public HashSet<Role> getRoles()
    {
        return roles;
    }

    public void setRoles(HashSet< Role> roles)
    {
        this.roles = roles;
        for (Role role : roles)
        {
            SimplePrincipal p = new SimplePrincipal(role.name);
            if (role.isRead())
                read.add(p);
            if (role.isWrite())
                write.add(p);
            if (role.isCreate())
                create.add(p);
        }
    }

} // SecurityMetadata
