/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2012-2014 The eXist Project
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
package org.exist.xquery.functions.system;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;
import org.exist.dom.QName;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

public class FnSnapshot extends BasicFunction {

	protected final static Logger logger = Logger.getLogger(FnSnapshot.class);
	
	protected final static QName NAME = 
		new QName("snapshot", SystemModule.NAMESPACE_URI, SystemModule.PREFIX);
	
	protected final static String DESCRIPTION =
		"Snapshot the database data folder (admin user only).";

	protected final static FunctionParameterSequenceType DIRorFILE = 
		new FunctionParameterSequenceType("dir-or-file", Type.STRING, Cardinality.EXACTLY_ONE,
		"This is either a backup directory with the backup descriptor (__contents__.xml) or a backup ZIP file.");

	protected final static  FunctionReturnSequenceType RESULT = 
		new FunctionReturnSequenceType(Type.INTEGER, Cardinality.EXACTLY_ONE, "the shapshot process return code");

	public final static FunctionSignature signatures[] = {
		new FunctionSignature(
			NAME,
			DESCRIPTION,
			new SequenceType[] {
				DIRorFILE,
			},
			new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, "the export results")
		)
	};

	public final static QName EXPORT_ELEMENT = new QName("export", SystemModule.NAMESPACE_URI, SystemModule.PREFIX);

	public FnSnapshot(XQueryContext context, FunctionSignature signature) {
		super(context, signature);
	}

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        if( !context.getSubject().hasDbaRole() ) {
            throw new XPathException( this, "Permission denied, calling user '" + context.getSubject().getName() + "' must be a DBA to kill a running xquery" );
        }

    	final String dirOrFile = args[0].getStringValue();
        
        DBBroker broker = context.getBroker();
        BrokerPool db = broker.getBrokerPool();
        
        File dataFolder = broker.getDataFolder();
        
        try {
            db.enterServiceMode(broker);
            
            Process p = Runtime.getRuntime().exec("cp -R " + dataFolder.getAbsolutePath() + " " + dirOrFile);
            
            p.waitFor();
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";                       
            while ((line = reader.readLine())!= null) {
                //output.append(line + "\n");
            }
            
            return new IntegerValue( p.exitValue() );
            
        } catch (final Exception e) {
            throw new XPathException(this, "snapshot failed with exception: " + e.getMessage(), e);
        } finally {
            try {
                db.exitServiceMode(broker);
            } catch (PermissionDeniedException e) {
                //can't be
                LOG.error(e, e);
            }
        }
    }
}