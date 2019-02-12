/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2016 The eXist Project
 * http://exist-db.org
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.xquery.restore.deferred;

import org.exist.security.ACLPermission.ACE_ACCESS_TYPE;
import org.exist.security.ACLPermission.ACE_TARGET;
import org.exist.xmldb.XmldbURI;

/**
 * Represents the permissions for a skipped entry in the restore process, e.g. apply() does nothing
 *
 * @author  Adam Retter <adam@exist-db.org>
 */
public class SkippedEntryDeferredPermission implements DeferredPermission {

    @Override
    public XmldbURI url() {
        return null;
    }

    @Override
    public void uuid(String uuid) {
    }

    @Override
    public void apply() {
    }

    @Override
    public void addACE(int index, ACE_TARGET target, String who, ACE_ACCESS_TYPE access_type, int mode) {
    }
}