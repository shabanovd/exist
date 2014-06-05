/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2014 The eXist Project
 *  http://exist-db.org
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.revisions;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public interface Constants {

    final static Charset ENCODING = StandardCharsets.UTF_8;
    final static TimeZone GMT = TimeZone.getTimeZone("GMT+0:00");
    
    final static String FILE_META = "metas.xml";
    
    final static String NONE = "NONE";
    
    final static String PARENT_UUID = "parent-uuid";
    
    final static String EL_FILE_NAME = "file-name";
    final static String EL_FILE_PATH = "file-path";

    final static String EL_META_TYPE = "meta-type";

    final static String EL_CREATED = "created";
    final static String EL_LAST_MODIFIED = "lastModified";
    
    final static String EL_PERMISSION = "permission";
    
    final static String AT_OWNER = "owner";
    final static String AT_GROUP = "group";
    final static String AT_MODE = "mode";

    final static String EL_ACL = "acl";
    
    final static String AT_VERSION = "version";

    final static String EL_ACE = "ace";
        
    final static String AT_INDEX = "index";
    final static String AT_TARGET = "target";
    final static String AT_WHO = "who";
    final static String AT_ACCESS_TYPE = "access_type";
    
    final static String EL_METASTORAGE = "metastorage";
    final static String EL_UUID = "uuid";
    
}
