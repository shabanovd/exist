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
package org.exist.rcs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class Revision implements Comparable<Revision> {
    
    Resource resource;
    long id;
    
    Path location;

    protected Revision(Resource resource, Path location) {
        
        id = Long.parseLong( location.getFileName().toString() );
        
        this.resource = resource;
        this.location = location;
    }

    @Override
    public int compareTo(Revision o) {
        return Long.compare(id, o.id);
    }
    
    public boolean isCollection() {
        return !(
                Files.exists(location.resolve("data.xml")) 
                || Files.exists(location.resolve("data.bin"))
            );
    }

    public boolean isXML() {
        return Files.exists(location.resolve("data.xml"));
    }
    
    public OutputStream getData() throws IOException {
        if (Files.exists(location.resolve("data.xml"))) {
            return Files.newOutputStream(location.resolve("data.xml"));
        }
        
        if (Files.exists(location.resolve("data.xml"))) {
            return Files.newOutputStream(location.resolve("data.bin"));
        }
        
        return null;
    }
}
