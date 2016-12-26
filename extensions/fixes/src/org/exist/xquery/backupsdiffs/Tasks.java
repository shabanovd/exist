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
package org.exist.xquery.backupsdiffs;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class Tasks {

    AtomicLong count = new AtomicLong();

    ExecutorService executor = Executors.newFixedThreadPool(4);

    public void waitTillDone() throws InterruptedException {
        while (count.get() > 0) {
            System.out.println("tasks till done: "+count.get());
            Thread.sleep(10 * 1000);
        }
        executor.shutdownNow();
    }

    public void submit(Callable<Boolean> callable) {
        count.incrementAndGet();

        try {
            executor.submit(() -> {
                try {
                    return callable.call();
                } finally {
                    count.decrementAndGet();
                }
            });
        } catch (Throwable ex) {
            count.decrementAndGet();
        }
    }
}
