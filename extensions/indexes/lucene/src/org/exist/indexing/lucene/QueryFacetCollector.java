/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2013 The eXist Project
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
 *
 *  $Id$
 */
package org.exist.indexing.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.encoding.DGapVInt8IntDecoder;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.search.CountingFacetsAggregator;
import org.apache.lucene.facet.search.DepthOneFacetResultsHandler;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetResultsHandler;
import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.facet.search.FastCountingFacetsAggregator;
import org.apache.lucene.facet.search.OrdinalValueResolver;
import org.apache.lucene.facet.search.TopKFacetResultsHandler;
import org.apache.lucene.facet.search.TopKInEachNodeHandler;
import org.apache.lucene.facet.search.FacetRequest.ResultMode;
import org.apache.lucene.facet.search.FacetRequest.SortOrder;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;
import org.exist.dom.DefaultDocumentSet;
import org.exist.dom.DocumentSet;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public abstract class QueryFacetCollector extends Collector {

    protected Scorer scorer;

    protected AtomicReaderContext context;
    protected AtomicReader reader;
    protected NumericDocValues docIdValues;

    protected final DocumentSet docs;

    protected final List<MatchingDocs> matchingDocs = new ArrayList<MatchingDocs>();
    protected final FacetArrays facetArrays;
    
    protected final TaxonomyReader taxonomyReader;
    protected final FacetSearchParams searchParams;

    protected int totalHits;
    protected FixedBitSet bits;
    protected float[] scores;

    protected DefaultDocumentSet docbits;
    //private FixedBitSet docbits;

    protected QueryFacetCollector(
            final DocumentSet docs, 
            
            final FacetSearchParams searchParams, 

            final TaxonomyReader taxonomyReader) {
        
        this.docs = docs;
        
        this.searchParams = searchParams;
        this.taxonomyReader = taxonomyReader;
        
//        this.facetArrays = new FacetArrays(taxonomyReader.getSize());
        this.facetArrays = new FacetArrays(
                PartitionsUtils.partitionSize(searchParams.indexingParams, taxonomyReader));
        
        docbits = new DefaultDocumentSet(1031);//docs.getDocumentCount());
        //docbits = new FixedBitSet(docs.getDocumentCount());

    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
    }

    @Override
    public void setNextReader(AtomicReaderContext atomicReaderContext)
            throws IOException {
        reader = atomicReaderContext.reader();
        docIdValues = reader.getNumericDocValues(LuceneUtil.FIELD_DOC_ID);

        if (bits != null) {
            matchingDocs.add(new MatchingDocs(context, bits, totalHits, scores));
        }
        bits = new FixedBitSet(reader.maxDoc());
        totalHits = 0;
        scores = new float[64]; // some initial size
        context = atomicReaderContext;
    }
    
    protected void finish() {
      if (bits != null) {
        matchingDocs.add(new MatchingDocs(this.context, bits, totalHits, scores));
        bits = null;
        scores = null;
        context = null;
      }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }

    @Override
    public abstract void collect(int doc);

    private boolean verifySearchParams(FacetSearchParams fsp) {
        // verify that all category lists were encoded with DGapVInt
        for (FacetRequest fr : fsp.facetRequests) {
            CategoryListParams clp = fsp.indexingParams.getCategoryListParams(fr.categoryPath);
            if (clp.createEncoder().createMatchingDecoder().getClass() != DGapVInt8IntDecoder.class) {
                return false;
            }
        }

        return true;
    }

    private FacetsAggregator getAggregator() {
        if (verifySearchParams(searchParams)) {
            return new FastCountingFacetsAggregator();
        } else {
            return new CountingFacetsAggregator();
        }
    }

    private Map<CategoryListParams,List<FacetRequest>> groupRequests() {
        if (searchParams.indexingParams.getAllCategoryListParams().size() == 1) {
          return Collections.singletonMap(searchParams.indexingParams.getCategoryListParams(null), searchParams.facetRequests);
        }
        
        HashMap<CategoryListParams,List<FacetRequest>> requestsPerCLP = new HashMap<CategoryListParams,List<FacetRequest>>();
        for (FacetRequest fr : searchParams.facetRequests) {
          CategoryListParams clp = searchParams.indexingParams.getCategoryListParams(fr.categoryPath);
          List<FacetRequest> requests = requestsPerCLP.get(clp);
          if (requests == null) {
            requests = new ArrayList<FacetRequest>();
            requestsPerCLP.put(clp, requests);
          }
          requests.add(fr);
        }
        return requestsPerCLP;
    }

    private FacetResultsHandler createFacetResultsHandler(FacetRequest fr, OrdinalValueResolver resolver) {
        if (fr.getDepth() == 1 && fr.getSortOrder() == SortOrder.DESCENDING) {
          return new DepthOneFacetResultsHandler(taxonomyReader, fr, facetArrays, resolver);
        }

        if (fr.getResultMode() == ResultMode.PER_NODE_IN_TREE) {
          return new TopKInEachNodeHandler(taxonomyReader, fr, resolver, facetArrays);
        } else {
          return new TopKFacetResultsHandler(taxonomyReader, fr, resolver, facetArrays);
        }
    }

    private static FacetResult emptyResult(int ordinal, FacetRequest fr) {
        FacetResultNode root = new FacetResultNode(ordinal, 0);
        root.label = fr.categoryPath;
        return new FacetResult(fr, root, 0);
    }
    
    List<FacetResult> facetResults = null;
    
    public List<FacetResult> getFacetResults() throws IOException {
        if (facetResults == null) {
            finish();
            facetResults = accumulate();
        }
        return facetResults;
    }

    private List<FacetResult> accumulate() throws IOException {
        
        // aggregate facets per category list (usually onle one category list)
        FacetsAggregator aggregator = getAggregator();
        for (CategoryListParams clp : groupRequests().keySet()) {
          for (MatchingDocs md : matchingDocs) {
            aggregator.aggregate(md, clp, facetArrays);
          }
        }
        
        ParallelTaxonomyArrays arrays = taxonomyReader.getParallelTaxonomyArrays();
        
        // compute top-K
        final int[] children = arrays.children();
        final int[] siblings = arrays.siblings();
        List<FacetResult> res = new ArrayList<FacetResult>();
        for (FacetRequest fr : searchParams.facetRequests) {
          int rootOrd = taxonomyReader.getOrdinal(fr.categoryPath);
          if (rootOrd == TaxonomyReader.INVALID_ORDINAL) { // category does not exist
            // Add empty FacetResult
            res.add(emptyResult(rootOrd, fr));
            continue;
          }
          CategoryListParams clp = searchParams.indexingParams.getCategoryListParams(fr.categoryPath);
          if (fr.categoryPath.length > 0) { // someone might ask to aggregate the ROOT category
            OrdinalPolicy ordinalPolicy = clp.getOrdinalPolicy(fr.categoryPath.components[0]);
            if (ordinalPolicy == OrdinalPolicy.NO_PARENTS) {
              // rollup values
              aggregator.rollupValues(fr, rootOrd, children, siblings, facetArrays);
            }
          }
          
          FacetResultsHandler frh = createFacetResultsHandler(fr, aggregator.createOrdinalValueResolver(fr, facetArrays));
          res.add(frh.compute());
        }
        return res;
    }
}

