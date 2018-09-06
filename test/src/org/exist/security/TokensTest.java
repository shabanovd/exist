/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2018 The eXist Project
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
package org.exist.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.List;
import org.exist.jetty.JettyStart;
import org.exist.security.internal.TokenRecord;
import org.exist.security.internal.TokensManager;
import org.exist.storage.BrokerPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xmldb.api.base.XMLDBException;

public class TokensTest {

  private JettyStart server;

  private void startServer() {
    if (server != null) return;

    System.out.println("Starting standalone server...");
    server = new JettyStart();
    server.run();
  }

  private void stopServer() {
    if (server == null) return;

    System.out.println("Shutdown standalone server...");
    server.shutdown();
    server = null;
  }

  private void restartServer() {
    stopServer();
    startServer();
  }

  @Test
  public void createAndInvalidateToken() throws Exception {

    restartServer();

    SecurityManager sm = BrokerPool.getInstance().getSecurityManager();

    TokensManager tm = sm.tokenManager();

    String token = tm.createToken("test", sm.getAccount("admin"));

    Subject s1 = tm.authenticate("admin", token, true);

    assertNotNull(s1);
    assertEquals("admin", s1.getUsername());

    Subject s2 = tm.authenticate("admin", token);

    assertNull(s2);
  }

  @Test
  public void createAndRestart() throws Exception {

    restartServer();

    SecurityManager sm = BrokerPool.getInstance().getSecurityManager();

    TokensManager tm = sm.tokenManager();

    Account account = sm.getAccount("admin");

    String token = tm.createToken("test", account);

    List<TokenRecord> l1 = tm.list(account);

    restartServer();

    sm = BrokerPool.getInstance().getSecurityManager();

    tm = sm.tokenManager();

    account = sm.getAccount("admin");

    List<TokenRecord> l2 = tm.list(account);

    assertEquals(l1.size(), l2.size());

    TokenRecord t1 = l1.get(0);
    TokenRecord t2 = l2.get(0);

    assertEquals(t1.token(), t2.token());
    assertEquals(t1.name(), t2.name());
    assertEquals(t1.account(), t2.account());
    assertEquals(t1.realm(), t2.realm());
    assertEquals(t1.created(), t2.created());
    assertEquals(t1.lastUse(), t2.lastUse());
  }
}
