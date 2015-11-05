/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2015 The eXist Project
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
package org.exist.security.internal;

import gnu.crypto.hash.RipeMD160;
import org.apache.commons.codec.binary.Base64;
import org.exist.Database;
import org.exist.EXistException;
import org.exist.config.Configurable;
import org.exist.config.Configuration;
import org.exist.config.annotation.ConfigurationClass;
import org.exist.config.annotation.ConfigurationFieldAsElement;
import org.exist.config.annotation.ConfigurationFieldClassMask;
import org.exist.scheduler.JobDescription;
import org.exist.security.*;
import org.exist.security.realm.Realm;
import org.exist.storage.DBBroker;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SimpleTrigger;

import java.security.SecureRandom;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
@ConfigurationClass("tokens")
public class TokensManager implements Configurable {

    public final static long TIMEOUT_CHECK_PERIOD = 1000; //10 * 60 * 1000; //10 minutes

    SecurityManagerImpl sm;

    Configuration configuration;

    @ConfigurationFieldAsElement("record")
    @ConfigurationFieldClassMask("org.exist.security.internal.TokenRecord")
    List<TokenRecord> tokens = new ArrayList<>();

    private static class Holder {
        static final SecureRandom numberGenerator = new SecureRandom();
    }

    private static final char[] TABLE = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
    };

    TokensManager(SecurityManagerImpl sm) {
        this.sm = sm;

        final Properties params = new Properties();
        params.put(getClass().getName(), this);

        sm.pool.getScheduler().createPeriodicJob(TIMEOUT_CHECK_PERIOD, new TokensCheck(),
            TIMEOUT_CHECK_PERIOD, params, SimpleTrigger.REPEAT_INDEFINITELY, false
        );
    }

    public String createToken(Account account) {

        TokenRecord token = new TokenRecord();

        SecureRandom ng = Holder.numberGenerator;

        StringBuilder sb = new StringBuilder(64);
        for (int i = 0; i < 64; i++) {
            sb.append(TABLE[ng.nextInt(TABLE.length)]);
        }

        String original_token = sb.toString();

        token.token = hash(original_token);

        token.created = System.currentTimeMillis();

        token.realm = account.getRealmId();
        token.account = account.getName();

        tokens.add(token);
        save();
        flush();

        return original_token;
    }

    public List<TokenRecord> list(Account account) {
        return tokens.stream()
            .filter(record -> account.getName().equals(record.account) && account.getRealmId().equals(record.realm))
            .map(TokenRecord::new)
            .collect(Collectors.toList());
    }

    public boolean invalidateToken(String token) {
        Iterator<TokenRecord> it = tokens.iterator();
        while (it.hasNext()) {
            TokenRecord record = it.next();

            if (token.equals(record.token)) {
                it.remove();
                save();
                flush();
                return true;
            }
        }

        return false;
    }

    public Subject authenticate(final String username, final Object credentials) throws AuthenticationException {

        if (credentials == null) return null;

        String token;
        if(credentials instanceof char[]) {
            token = String.valueOf((char[]) credentials);
        } else {
            token = hash(credentials.toString());
        }

        for (TokenRecord record : tokens) {
            if (token.equals(record.token)) {
                record.lastUse = System.currentTimeMillis();
                save();

                for(Realm realm : sm.realms) {
                    if (!record.realm.equals(realm.getId())) continue;

                    Account account = realm.getAccount(record.account);
                    if (account != null) {
                        if (username != null && !username.equals(account.getUsername())) {
                            slowDown();
                            throw new AuthenticationException(
                                AuthenticationException.ACCOUNT_NOT_FOUND,
                                "Token reference to account '" + account.getUsername() + "' " +
                                "that miss match provided username '" + username + "'"
                            );
                        }
                        return new SubjectAccreditedImpl((AccountImpl) account, token);
                    }
                }

                slowDown();
                throw new AuthenticationException(
                    AuthenticationException.ACCOUNT_NOT_FOUND,
                    "Token reference to account that not found");
            }
        }

        return null;
    }

    private void slowDown() {
        //slow down a bit
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            //ignore
        }
    }

    boolean dirty = false;

    private void save() {
        dirty = true;
    }

    private void flush() {
        if (configuration == null || !dirty) return;

        dirty = false;
        try {
            executeAsSystemUser(sm.getDatabase(), broker -> {
                try {
                    configuration.save();
                } catch (Exception e) {
                    dirty = true;
                    SecurityManagerImpl.LOG.error(e.getMessage(), e);
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            SecurityManagerImpl.LOG.error(e.getMessage(), e);
        }
    }

    final String hash(String p) {
        final RipeMD160 ripemd160 = new RipeMD160();
        final byte[] data = p.getBytes();
        ripemd160.update(data, 0, data.length);
        final byte[] hash = ripemd160.digest();
        return Base64.encodeBase64String(hash);
    }

    void shutdown() {
        flush();
    }

    @Override
    public boolean isConfigured() {
        return configuration != null;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    public static class TokensCheck implements JobDescription, org.quartz.Job {

        public TokensCheck() {
        }

        @Override
        public void execute(JobExecutionContext jec) throws JobExecutionException {

            JobDataMap jobDataMap = jec.getJobDetail().getJobDataMap();

            Map params = (Map)jobDataMap.get("params");
            if (params == null) return;

            TokensManager tm = ( TokensManager )params.get( TokensManager.class.getName() );

            if (tm != null) tm.flush();
        }

        @Override
        public String getGroup() {
            return "eXist.Security";
        }

        @Override
        public String getName() {
            return "Tokens.Check";
        }

        @Override
        public void setName(String name) {
        }
    }

    public void executeAsSystemUser(Database db, Consumer<DBBroker> unit) throws EXistException, PermissionDeniedException {
        DBBroker broker = null;
        final Subject currentSubject = db.getSubject();
        try {
            //elevate to system privs
            broker = db.get(db.getSecurityManager().getSystemSubject());

            unit.accept(broker);
        } finally {
            if(broker != null) {
                broker.setSubject(currentSubject);
                db.release(broker);
            }
        }
    }

}
