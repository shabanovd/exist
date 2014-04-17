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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class RCSResource {
    
    Path location;

    String uuid;

    protected RCSResource(Path location) {
        
        uuid = location.getFileName().toString();
        
        this.location = location;
    }
    
    public List<Revision> revisions() throws IOException {
        List<Revision> revs = new ArrayList<Revision>();
        
        try (DirectoryStream<Path> dirs = Files.newDirectoryStream(location)) {
            for (Path dir : dirs) {
                revs.add(new Revision(this, dir));
            }
        }
        
        Collections.sort(revs, Collections.reverseOrder());
        
        return revs;
    }

    public Revision lastRevision() throws IOException {
        
        List<Revision> revs = revisions();
        
        if (revs.size() < 1) return null;
        
        return revisions().get(0);
    }
}
