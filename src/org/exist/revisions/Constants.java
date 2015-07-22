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
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public interface Constants {

    Charset ENCODING = StandardCharsets.UTF_8;
    TimeZone GMT = TimeZone.getTimeZone("GMT+0:00");

    String XML = "xml";
    String BIN = "bin";
    String COL = "col";
    String DEL = "del";
    
    String NONE = "NONE";

    String EL_LOG_PATH = "log";
    String EL_DATA_HASH = "hash";
    String EL_RESOURCE_TYPE = "type";
    
    String PARENT_UUID = "parent-uuid";
    
    String EL_FILE_NAME = "file-name";
    String EL_FILE_PATH = "file-path";

    String EL_META_TYPE = "meta-type";

    String EL_CREATED = "created";
    String EL_LAST_MODIFIED = "lastModified";
    
    String EL_PERMISSION = "permission";
    
    String AT_OWNER = "owner";
    String AT_GROUP = "group";
    String AT_MODE = "mode";

    String EL_ACL = "acl";
    
    String AT_VERSION = "version";

    String EL_ACE = "ace";
        
    String AT_INDEX = "index";
    String AT_TARGET = "target";
    String AT_WHO = "who";
    String AT_ACCESS_TYPE = "access_type";
    
    String EL_METASTORAGE = "metastorage";
    String EL_UUID = "uuid";

    String RESTORE_UUID = "RESTORE_UUID";
    String RESTORE_DOCTYPE = "RESTORE_DOCTYPE";

    String YES = "YES";
    String NO = "NO";

    Map<String, String> EMPTY_MAP = new HashMap<>();
}
