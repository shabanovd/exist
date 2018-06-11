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

package org.exist.xquery.functions.securitymanager;

import com.evolvedbinary.j8fu.function.Runnable4E;
import org.exist.EXistException;
import org.exist.TestUtils;
import org.exist.collections.Collection;
import org.exist.collections.IndexInfo;
import org.exist.collections.triggers.TriggerException;
import org.exist.dom.persistent.BinaryDocument;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.persistent.LockedDocument;
import org.exist.security.*;
import org.exist.security.SecurityManager;
import org.exist.security.internal.aider.GroupAider;
import org.exist.security.internal.aider.UserAider;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.storage.txn.Txn;
import org.exist.test.ExistEmbeddedServer;
import org.exist.test.TestConstants;
import org.exist.util.Configuration;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQuery;
import org.exist.xquery.value.Sequence;
import org.junit.*;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PermissionsFunctionChownTest {

    private static final boolean RESTRICTED = true;
    private static final boolean NOT_RESTRICTED = false;

    private static final boolean IS_SET = true;
    private static final boolean NOT_SET = false;

    private static final String USER1_NAME = "user1";
    private static final String USER1_PWD = USER1_NAME;
    private static final String USER2_NAME = "user2";
    private static final String USER2_PWD = USER2_NAME;

    private static final XmldbURI USER1_COL1 = XmldbURI.create("u1c1");
    private static final XmldbURI USER1_COL2 = XmldbURI.create("u1c2");
    private static final XmldbURI USER1_DOC1 = XmldbURI.create("u1d1.xml");
    private static final XmldbURI USER1_XQUERY1 = XmldbURI.create("u1xq1.xq");

    private static final String OTHER_GROUP_NAME = "otherGroup";

    @ClassRule
    public static final ExistEmbeddedServer existWebServer = new ExistEmbeddedServer(true, true);

    /**
     * With {@code posix-chown-restricted="true"},
     * as a DBA user change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentOwnerToSelfAsDBA_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeOwner(adminUser, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as a DBA user change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentOwnerToSelfAsDBA() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeOwner(adminUser, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as a DBA user change the owner of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionOwnerToSelfAsDBA_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeOwner(adminUser, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as a DBA user change the owner of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionOwnerToSelfAsDBA() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeOwner(adminUser, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the document owner user change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentOwnerToSelfAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeOwner(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the document owner user change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentOwnerToSelfAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeOwner(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the collection owner user change the owner of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionOwnerToSelfAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeOwner(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the collection owner user change the owner of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionOwnerToSelfAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeOwner(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the document's owner) change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentOwnerToSelfAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        changeOwner(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the document's owner) change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentOwnerToSelfAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        changeOwner(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the collection's owner) change the owner of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionOwnerToSelfAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        changeOwner(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the collection's owner) change the owner of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionOwnerToSelfAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        changeOwner(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as a DBA user change the owner of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test
    public void changeDocumentOwnerAsDBA_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeOwner(adminUser, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as a DBA user change the owner of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test
    public void changeDocumentOwnerAsDBA() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeOwner(adminUser, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as a DBA user change the owner of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test
    public void changeCollectionOwnerAsDBA_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeOwner(adminUser, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as a DBA user change the owner of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test
    public void changeCollectionOwnerAsDBA() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeOwner(adminUser, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the document owner user change the owner of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeDocumentOwnerAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
            changeOwner(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the document owner user change the owner of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test
    public void changeDocumentOwnerAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeOwner(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the collection owner user change the owner of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeCollectionOwnerAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
            changeOwner(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the collection owner user change the owner of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test
    public void changeCollectionOwnerAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeOwner(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the document's owner) change the owner of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeDocumentOwnerAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeOwner(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the document's owner) change the owner of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeDocumentOwnerAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeOwner(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the collection's owner) change the owner of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeCollectionOwnerAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeOwner(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the collection's owner) change the owner of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeCollectionOwnerAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeOwner(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the document owner user change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     * Finally make sure that chown has cleared the setUid and setGid bits.
     */
    @Test
    public void changeDocumentOwnerToSelfAsNonDbaOwner_clearsSetUidAndSetGid_restricted() throws AuthenticationException, EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().authenticate(USER1_NAME, USER1_PWD);

        // check the setUid and setGid bits are set before we begin
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);

        // change the owner
        changeOwner(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), USER1_NAME);

        // check the setUid and setGid bits are now cleared
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), NOT_SET);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the document owner user change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     * Finally make sure that chown has cleared the setUid and setGid bits.
     */
    @Test
    public void changeDocumentOwnerToSelfAsNonDbaOwner_clearsSetUidAndSetGid() throws AuthenticationException, EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().authenticate(USER1_NAME, USER1_PWD);

        // check the setUid and setGid bits are set before we begin
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);

        // change the owner
        changeOwner(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), USER1_NAME);

        // check the setUid and setGid bits are now cleared
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), NOT_SET);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the collection owner user change the owner of {@link #USER1_COL2} from "user1" to "user1".
     * Finally make sure that chown has cleared the setUid and setGid bits.
     */
    @Test
    public void changeCollectionOwnerToSelfAsNonDbaOwner_clearsSetUidAndSetGid_restricted() throws AuthenticationException, EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().authenticate(USER1_NAME, USER1_PWD);

        // check the setUid and setGid bits are set before we begin
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);

        // change the owner
        changeOwner(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), USER1_NAME);

        // check the setUid and setGid bits are now cleared
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), NOT_SET);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the collection owner user change the owner of {@link #USER1_COL2} from "user1" to "user1".
     * Finally make sure that chown has cleared the setUid and setGid bits.
     */
    @Test
    public void changeCollectionOwnerToSelfAsNonDbaOwner_clearsSetUidAndSetGid() throws AuthenticationException, EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().authenticate(USER1_NAME, USER1_PWD);

        // check the setUid and setGid bits are set before we begin
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);

        // change the owner
        changeOwner(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), USER1_NAME);

        // check the setUid and setGid bits are now cleared
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), NOT_SET);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the DBA user change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     * Finally make sure that chown has preserved the setUid and setGid bits.
     */
    @Test
    public void changeDocumentOwnerToSelfAsDba_preservesSetUidAndSetGid_restricted() throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().getSystemSubject();

        // check the setUid and setGid bits are set before we begin
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);

        // change the owner
        changeOwner(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), USER1_NAME);

        // check the setUid and setGid bits are still set
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the DBA user change the owner of {@link #USER1_DOC1} from "user1" to "user1".
     * Finally make sure that chown has preserved the setUid and setGid bits.
     */
    @Test
    public void changeDocumentOwnerToSelfAsDba_preservesSetUidAndSetGid() throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().getSystemSubject();

        // check the setUid and setGid bits are set before we begin
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);

        // change the owner
        changeOwner(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), USER1_NAME);

        // check the setUid and setGid bits are still set
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the DBA user change the owner of {@link #USER1_COL2} from "user1" to "user1".
     * Finally make sure that chown has preserved the setUid and setGid bits.
     */
    @Test
    public void changeCollectionOwnerToSelfAsDba_preservesSetUidAndSetGid_restricted() throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().getSystemSubject();

        // check the setUid and setGid bits are set before we begin
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);

        // change the owner
        changeOwner(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), USER1_NAME);

        // check the setUid and setGid bits are still set
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the DBA user change the owner of {@link #USER1_COL2} from "user1" to "user1".
     * Finally make sure that chown has preserved the setUid and setGid bits.
     */
    @Test
    public void changeCollectionOwnerToSelfAsDba_preservesSetUidAndSetGid() throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().getSystemSubject();

        // check the setUid and setGid bits are set before we begin
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);

        // change the owner
        changeOwner(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), USER1_NAME);

        // check the setUid and setGid bits are still set
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as a DBA user change the group of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentGroupToSelfAsDBA_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeGroup(adminUser, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as a DBA user change the group of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentGroupToSelfAsDBA() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeGroup(adminUser, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as a DBA user change the group of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionGroupToSelfAsDBA_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeGroup(adminUser, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as a DBA user change the group of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionGroupToSelfAsDBA() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeGroup(adminUser, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the document owner user change the group of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentGroupToSelfAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the document owner user change the group of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentGroupToSelfAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the collection owner user change the group of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionGroupToSelfAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the collection owner user change the group of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionGroupToSelfAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the document's owner) change the group of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentGroupToSelfAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        changeGroup(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the document's owner) change the group of {@link #USER1_DOC1} from "user1" to "user1".
     */
    @Test
    public void changeDocumentGroupToSelfAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        changeGroup(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the collection's owner) change the group of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionGroupToSelfAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        changeGroup(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the collection's owner) change the group of {@link #USER1_COL1} from "user1" to "user1".
     */
    @Test
    public void changeCollectionGroupToSelfAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        changeGroup(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER1_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as a DBA user change the group of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test
    public void changeDocumentGroupAsDBA_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeGroup(adminUser, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as a DBA user change the group of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test
    public void changeDocumentGroupAsDBA() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeGroup(adminUser, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as a DBA user change the group of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test
    public void changeCollectionGroupAsDBA_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeGroup(adminUser, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as a DBA user change the group of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test
    public void changeCollectionGroupAsDBA() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject adminUser = existWebServer.getBrokerPool().getSecurityManager().authenticate(TestUtils.ADMIN_DB_USER, TestUtils.ADMIN_DB_PWD);
        changeGroup(adminUser, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the document owner user change the group of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeDocumentGroupAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
            changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the document owner user change the group of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test
    public void changeDocumentGroupAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the collection owner user change the group of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeCollectionGroupAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
            changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the collection owner user change the group of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test
    public void changeCollectionGroupAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the document's owner) change the group of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeDocumentGroupAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeGroup(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the document's owner) change the group of {@link #USER1_DOC1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeDocumentGroupAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeGroup(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the collection's owner) change the group of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeCollectionGroupAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeGroup(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the collection's owner) change the group of {@link #USER1_COL1} from "user1" to "user2".
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeCollectionGroupAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeGroup(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), USER2_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the document owner user change the group of {@link #USER1_DOC1} from "user1" to "otherGroup" (of which user1 is a member).
     */
    @Test
    public void changeDocumentGroupToMemberGroupAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), OTHER_GROUP_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the document owner user change the group of {@link #USER1_DOC1} from "user1" to "otherGroup" (of which user1 is a member).
     */
    @Test
    public void changeDocumentGroupToMemberGroupAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), OTHER_GROUP_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the collection owner user change the group of {@link #USER1_COL1} from "user1" to "otherGroup" (of which user1 is a member).
     */
    @Test
    public void changeCollectionGroupToMemberGroupAsNonDBAOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), OTHER_GROUP_NAME);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the collection owner user change the group of {@link #USER1_COL1} from "user1" to "otherGroup" (of which user1 is a member).
     */
    @Test
    public void changeCollectionGroupToMemberGroupAsNonDBAOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user1 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        changeGroup(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), OTHER_GROUP_NAME);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the document's owner) change the group of {@link #USER1_DOC1} from "user1" to "otherGroup" (of which user2 is a member).
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeDocumentGroupToMemberGroupAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeGroup(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), OTHER_GROUP_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the document's owner) change the group of {@link #USER1_DOC1} from "user1" to "otherGroup" (of which user2 is a member).
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeDocumentGroupToMemberGroupAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        extractPermissionDenied(() ->
                changeGroup(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1), OTHER_GROUP_NAME)
        );
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the user "user2" (not the collection's owner) change the group of {@link #USER1_COL1} from "user1" to "otherGroup" (of which user2 is a member).
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeCollectionGroupToMemberGroupAsNonOwner_restricted() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        extractPermissionDenied(() -> {
            final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
            changeGroup(user2, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), OTHER_GROUP_NAME);
        });
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the user "user2" (not the collection's owner) change the group of {@link #USER1_COL1} from "user1" to "otherGroup" (of which user2 is a member).
     */
    @Test(expected=PermissionDeniedException.class)
    public void changeCollectionGroupToMemberGroupAsNonOwner() throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        final Subject user2 = existWebServer.getBrokerPool().getSecurityManager().authenticate(USER2_NAME, USER2_PWD);
        extractPermissionDenied(() ->
                changeGroup(user2, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1), OTHER_GROUP_NAME)
        );
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the document owner user change the group of {@link #USER1_DOC1} from "user1" to "user1".
     * Finally make sure that chown has cleared the setUid and setGid bits.
     */
    @Test
    public void changeDocumentGroupToSelfAsNonDbaOwner_clearsSetUidAndSetGid_restricted() throws AuthenticationException, EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().authenticate(USER1_NAME, USER1_PWD);

        // check the setUid and setGid bits are set before we begin
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);

        // change the owner
        changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), USER1_NAME);

        // check the setUid and setGid bits are now cleared
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), NOT_SET);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the document owner user change the group of {@link #USER1_DOC1} from "user1" to "user1".
     * Finally make sure that chown has cleared the setUid and setGid bits.
     */
    @Test
    public void changeDocumentGroupToSelfAsNonDbaOwner_clearsSetUidAndSetGid() throws AuthenticationException, EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().authenticate(USER1_NAME, USER1_PWD);

        // check the setUid and setGid bits are set before we begin
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);

        // change the owner
        changeGroup(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), USER1_NAME);

        // check the setUid and setGid bits are now cleared
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), NOT_SET);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the collection owner user change the group of {@link #USER1_COL2} from "user1" to "user1".
     * Finally make sure that chown has cleared the setUid and setGid bits.
     */
    @Test
    public void changeCollectionGroupToSelfAsNonDbaOwner_clearsSetUidAndSetGid_restricted() throws AuthenticationException, EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().authenticate(USER1_NAME, USER1_PWD);

        // check the setUid and setGid bits are set before we begin
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);

        // change the owner
        changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), USER1_NAME);

        // check the setUid and setGid bits are now cleared
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), NOT_SET);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the collection owner user change the group of {@link #USER1_COL2} from "user1" to "user1".
     * Finally make sure that chown has cleared the setUid and setGid bits.
     */
    @Test
    public void changeCollectionGroupToSelfAsNonDbaOwner_clearsSetUidAndSetGid() throws AuthenticationException, EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().authenticate(USER1_NAME, USER1_PWD);

        // check the setUid and setGid bits are set before we begin
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);

        // change the owner
        changeGroup(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), USER1_NAME);

        // check the setUid and setGid bits are now cleared
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), NOT_SET);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the DBA user change the group of {@link #USER1_DOC1} from "user1" to "user1".
     * Finally make sure that chown has preserved the setUid and setGid bits.
     */
    @Test
    public void changeDocumentGroupToSelfAsDba_preservesSetUidAndSetGid_restricted() throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().getSystemSubject();

        // check the setUid and setGid bits are set before we begin
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);

        // change the owner
        changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), USER1_NAME);

        // check the setUid and setGid bits are still set
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the DBA user change the group of {@link #USER1_DOC1} from "user1" to "user1".
     * Finally make sure that chown has preserved the setUid and setGid bits.
     */
    @Test
    public void changeDocumentGroupToSelfAsDba_preservesSetUidAndSetGid() throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().getSystemSubject();

        // check the setUid and setGid bits are set before we begin
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);

        // change the owner
        changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), USER1_NAME);

        // check the setUid and setGid bits are still set
        assertDocumentSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_XQUERY1), IS_SET);
    }

    /**
     * With {@code posix-chown-restricted="true"},
     * as the DBA user change the group of {@link #USER1_COL2} from "user1" to "user1".
     * Finally make sure that chown has preserved the setUid and setGid bits.
     */
    @Test
    public void changeCollectionGroupToSelfAsDba_preservesSetUidAndSetGid_restricted() throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().getSystemSubject();

        // check the setUid and setGid bits are set before we begin
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);

        // change the owner
        changeGroup(user1, RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), USER1_NAME);

        // check the setUid and setGid bits are still set
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);
    }

    /**
     * With {@code posix-chown-restricted="false"},
     * as the DBA user change the owner of {@link #USER1_COL2} from "user1" to "user1".
     * Finally make sure that chown has preserved the setUid and setGid bits.
     */
    @Test
    public void changeCollectionGroupToSelfAsDba_preservesSetUidAndSetGid() throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final Subject user1 = pool.getSecurityManager().getSystemSubject();

        // check the setUid and setGid bits are set before we begin
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);

        // change the owner
        changeGroup(user1, NOT_RESTRICTED, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), USER1_NAME);

        // check the setUid and setGid bits are still set
        assertCollectionSetUidSetGid(user1, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2), IS_SET);
    }

    private void changeOwner(final Subject execAsUser, final boolean restricted, final XmldbURI uri, final String newOwner) throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();

        final boolean prevRestricted = setPosixChownRestricted(restricted);

        final String query =
                "import module namespace sm = 'http://exist-db.org/xquery/securitymanager';\n" +
                "sm:chown(xs:anyURI('" + uri.getRawCollectionPath() + "'), '" + newOwner + "'),\n" +
                "sm:get-permissions(xs:anyURI('" + uri.getRawCollectionPath() + "'))/sm:permission/string(@owner)";

        try (final DBBroker broker = pool.get(Optional.of(execAsUser))) {

            final XQuery xquery = existWebServer.getBrokerPool().getXQueryService();
            final Sequence result = xquery.execute(broker, query, null);

            assertEquals(1, result.getItemCount());
            assertEquals(newOwner, result.itemAt(0).getStringValue());
        } finally {
            setPosixChownRestricted(prevRestricted);
        }
    }

    private void changeGroup(final Subject execAsUser, final boolean restricted, final XmldbURI uri, final String newGroup) throws EXistException, PermissionDeniedException, XPathException {
        final BrokerPool pool = existWebServer.getBrokerPool();

        final boolean prevRestricted = setPosixChownRestricted(restricted);

        final String query =
                "import module namespace sm = 'http://exist-db.org/xquery/securitymanager';\n" +
                        "sm:chgrp(xs:anyURI('" + uri.getRawCollectionPath() + "'), '" + newGroup + "'),\n" +
                        "sm:get-permissions(xs:anyURI('" + uri.getRawCollectionPath() + "'))/sm:permission/string(@group)";

        try (final DBBroker broker = pool.get(Optional.of(execAsUser))) {

            final XQuery xquery = existWebServer.getBrokerPool().getXQueryService();
            final Sequence result = xquery.execute(broker, query, null);

            assertEquals(1, result.getItemCount());
            assertEquals(newGroup, result.itemAt(0).getStringValue());
        } finally {
            setPosixChownRestricted(prevRestricted);
        }
    }

    private static void assertDocumentSetUidSetGid(final Subject execAsUser, final XmldbURI uri, final boolean isSet) throws EXistException, PermissionDeniedException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        try (final DBBroker broker = pool.get(Optional.of(execAsUser));
                final LockedDocument lockedDoc = broker.getXMLResource(uri, Lock.LockMode.READ_LOCK)) {

            final DocumentImpl doc = lockedDoc.getDocument();
            if (isSet) {
                assertTrue(doc.getPermissions().isSetUid());
                assertTrue(doc.getPermissions().isSetGid());
            } else {
                assertFalse(doc.getPermissions().isSetUid());
                assertFalse(doc.getPermissions().isSetGid());
            }
        }
    }

    private static void assertCollectionSetUidSetGid(final Subject execAsUser, final XmldbURI uri, final boolean isSet) throws EXistException, PermissionDeniedException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        try (final DBBroker broker = pool.get(Optional.of(execAsUser))) {
            try (final Collection col = broker.openCollection(uri, Lock.LockMode.READ_LOCK);) {

                if (isSet) {
                    assertTrue(col.getPermissions().isSetUid());
                    assertTrue(col.getPermissions().isSetGid());
                } else {
                    assertFalse(col.getPermissions().isSetUid());
                    assertFalse(col.getPermissions().isSetGid());
                }
            }
        }
    }

    @BeforeClass
    public static void prepareDb() throws EXistException, PermissionDeniedException, IOException, TriggerException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final SecurityManager sm = pool.getSecurityManager();
        try (final DBBroker broker = pool.get(Optional.of(sm.getSystemSubject()));
                final Txn transaction = pool.getTransactionManager().beginTransaction()) {
            final Collection collection = broker.getOrCreateCollection(transaction, TestConstants.TEST_COLLECTION_URI);
            PermissionFactory.chmod(broker, collection, Optional.of(511), Optional.empty());
            broker.saveCollection(transaction, collection);

            createUser(broker, sm, USER1_NAME, USER1_PWD);
            createUser(broker, sm, USER2_NAME, USER2_PWD);

            final Group otherGroup = new GroupAider(OTHER_GROUP_NAME);
            sm.addGroup(broker, otherGroup);
            final Account user1 = sm.getAccount(USER1_NAME);
            user1.addGroup(OTHER_GROUP_NAME);
            sm.updateAccount(user1);
            final Account user2 = sm.getAccount(USER2_NAME);
            user2.addGroup(OTHER_GROUP_NAME);
            sm.updateAccount(user2);

            transaction.commit();
        }
    }

    @Before
    public void setup() throws EXistException, PermissionDeniedException, LockException, SAXException, IOException, AuthenticationException {
        final BrokerPool pool = existWebServer.getBrokerPool();

        // create user1 resources
        final Subject user1 = pool.getSecurityManager().authenticate(USER1_NAME, USER1_PWD);
        try (final DBBroker broker = pool.get(Optional.of(user1));
                final Txn transaction = pool.getTransactionManager().beginTransaction();
                final Collection collection = broker.openCollection(TestConstants.TEST_COLLECTION_URI, Lock.LockMode.WRITE_LOCK)) {

            final Collection u1c1 = broker.getOrCreateCollection(transaction, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1));
            broker.saveCollection(transaction, u1c1);

            final Collection u1c2 = broker.getOrCreateCollection(transaction, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2));
            PermissionFactory.chmod_str(broker, u1c2, Optional.of("u+s,g+s"), Optional.empty());
            broker.saveCollection(transaction, u1c2);

            final String xml1 = "<empty1/>";
            final IndexInfo indexInfo1 = collection.validateXMLResource(transaction, broker, USER1_DOC1, xml1);
            collection.store(transaction, broker, indexInfo1, xml1);

            final String xquery1 =
                    "import module namespace sm = 'http://exist-db.org/xquery/securitymanager';\n" +
                    "sm:id()";
            final BinaryDocument uqxq1 = collection.addBinaryResource(transaction, broker, USER1_XQUERY1, xquery1.getBytes(UTF_8), "application/xquery");
            PermissionFactory.chmod_str(broker, uqxq1, Optional.of("u+s,g+s"), Optional.empty());

            broker.saveCollection(transaction, collection);

            transaction.commit();
        }
    }

    @After
    public void teardown() throws EXistException, PermissionDeniedException, IOException, TriggerException, LockException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        try (final DBBroker broker = pool.get(Optional.of(pool.getSecurityManager().getSystemSubject()));
                final Txn transaction = pool.getTransactionManager().beginTransaction()) {

            removeDocument(broker, transaction, TestConstants.TEST_COLLECTION_URI.append(USER1_DOC1));
            removeCollection(broker, transaction, TestConstants.TEST_COLLECTION_URI.append(USER1_COL2));
            removeCollection(broker, transaction, TestConstants.TEST_COLLECTION_URI.append(USER1_COL1));

            transaction.commit();
        }
    }

    @AfterClass
    public static void cleanupDb() throws EXistException, PermissionDeniedException, IOException, TriggerException {
        final BrokerPool pool = existWebServer.getBrokerPool();
        final SecurityManager sm = pool.getSecurityManager();
        try (final DBBroker broker = pool.get(Optional.of(sm.getSystemSubject()));
             final Txn transaction = pool.getTransactionManager().beginTransaction()) {

            removeUser(sm, USER2_NAME);
            removeUser(sm, USER1_NAME);

            removeCollection(broker, transaction, TestConstants.TEST_COLLECTION_URI);

            transaction.commit();
        }
    }

    private static void extractPermissionDenied(final Runnable4E<AuthenticationException, XPathException, PermissionDeniedException, EXistException> runnable) throws AuthenticationException, XPathException, PermissionDeniedException, EXistException {
        try {
            runnable.run();
        } catch (final XPathException e) {
            if (e.getCause() != null && e.getCause() instanceof PermissionDeniedException) {
                throw (PermissionDeniedException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    private static void removeDocument(final DBBroker broker, final Txn transaction, final XmldbURI documentUri) throws PermissionDeniedException, LockException, IOException, TriggerException {
        try (final Collection collection = broker.openCollection(documentUri.removeLastSegment(), Lock.LockMode.WRITE_LOCK)) {
            collection.removeXMLResource(transaction, broker, documentUri.lastSegment());
            broker.saveCollection(transaction, collection);
        }
    }

    private static void removeCollection(final DBBroker broker, final Txn transaction, final XmldbURI collectionUri) throws PermissionDeniedException, IOException, TriggerException {
        try (final Collection collection = broker.openCollection(collectionUri, Lock.LockMode.WRITE_LOCK)) {
            broker.removeCollection(transaction, collection);
        }
    }

    private static void createUser(final DBBroker broker, final SecurityManager sm, final String username, final String password) throws PermissionDeniedException, EXistException {
        Group userGroup = new GroupAider(username);
        sm.addGroup(broker, userGroup);
        final Account user = new UserAider(username);
        user.setPassword(password);
        user.setPrimaryGroup(userGroup);
        sm.addAccount(user);

        userGroup = sm.getGroup(username);
        userGroup.addManager(sm.getAccount(username));
        sm.updateGroup(userGroup);
    }

    private static void removeUser(final SecurityManager sm, final String username) throws PermissionDeniedException, EXistException {
        sm.deleteAccount(username);
        sm.deleteGroup(username);
    }

    /**
     * Set the posix-chown-restricted flag.
     *
     * @param restricted true if the restriction is enforced, false otherwise.
     *
     * @return the previous value of the flag.
     */
    private boolean setPosixChownRestricted(final boolean restricted) {
        final Configuration config = existWebServer.getBrokerPool().getConfiguration();
        final boolean prevPosixChownRestricted = config.getProperty(DBBroker.POSIX_CHOWN_RESTRICTED_PROPERTY, true);
        config.setProperty(DBBroker.POSIX_CHOWN_RESTRICTED_PROPERTY, restricted);
        return prevPosixChownRestricted;
    }
}
