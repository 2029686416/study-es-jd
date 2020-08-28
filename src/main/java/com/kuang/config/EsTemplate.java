package com.kuang.config;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.*;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.ElasticsearchException;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.facet.FacetRequest;
import org.springframework.data.elasticsearch.core.geo.GeoBox;
import org.springframework.data.elasticsearch.core.geo.GeoPoint;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentProperty;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.data.elasticsearch.core.query.Criteria.OperationKey;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.client.Requests.indicesExistsRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.index.query.Operator.AND;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.springframework.util.CollectionUtils.isEmpty;

//import org.springframework.data.elasticsearch.core.CriteriaFilterProcessor;
//import org.springframework.data.elasticsearch.core.CriteriaQueryProcessor;
//import org.springframework.data.elasticsearch.core.ResourceUtil;
//import org.springframework.data.elasticsearch.core.StreamQueries;

public class EsTemplate extends ElasticsearchTemplate {

	private static final Logger QUERY_LOGGER = LoggerFactory
			.getLogger("org.springframework.data.elasticsearch.core.QUERY");
	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTemplate.class);

	private Client client;
	private ResultsMapper resultsMapper;
	private String searchTimeout;

	public EsTemplate(Client client) {
		this(client, new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext()));
	}

	public EsTemplate(Client client, EntityMapper entityMapper) {
		this(client, new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext()), entityMapper);
	}

	public EsTemplate(Client client, ElasticsearchConverter elasticsearchConverter,
                      EntityMapper entityMapper) {
		this(client, elasticsearchConverter,
				new DefaultResultMapper(elasticsearchConverter.getMappingContext(), entityMapper));
	}

	public EsTemplate(Client client, ResultsMapper resultsMapper) {
		this(client, new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext()), resultsMapper);
	}

	public EsTemplate(Client client, ElasticsearchConverter elasticsearchConverter) {
		this(client, elasticsearchConverter, new DefaultResultMapper(elasticsearchConverter.getMappingContext()));
	}

	public EsTemplate(Client client, ElasticsearchConverter elasticsearchConverter,
                      ResultsMapper resultsMapper) {

		super(client,elasticsearchConverter,resultsMapper);

		Assert.notNull(client, "Client must not be null!");
		Assert.notNull(resultsMapper, "ResultsMapper must not be null!");

		this.client = client;
		this.resultsMapper = resultsMapper;
	}


	@Override
	public <T> long count(SearchQuery searchQuery, Class<T> clazz) {
		QueryBuilder elasticsearchQuery = searchQuery.getQuery();
		QueryBuilder elasticsearchFilter = searchQuery.getFilter();

		if (elasticsearchFilter == null) {
			return doCount(prepareCount(searchQuery, clazz), elasticsearchQuery);
		} else {
			// filter could not be set into CountRequestBuilder, convert request into search request
			return doCount(prepareSearch(searchQuery, clazz), elasticsearchQuery, elasticsearchFilter);
		}
	}

	@Override
	public <T> long count(CriteriaQuery query) {
		return count(query, null);
	}

	@Override
	public <T> long count(SearchQuery query) {
		return count(query, null);
	}

	
	private long doCount(SearchRequestBuilder countRequestBuilder, QueryBuilder elasticsearchQuery) {

		if (elasticsearchQuery != null) {
			countRequestBuilder.setQuery(elasticsearchQuery);
		}
		countRequestBuilder.setSize(0);
		return countRequestBuilder.execute().actionGet().getHits().getTotalHits().value;
	}

	private long doCount(SearchRequestBuilder searchRequestBuilder, QueryBuilder elasticsearchQuery,
                         QueryBuilder elasticsearchFilter) {
		if (elasticsearchQuery != null) {
			searchRequestBuilder.setQuery(elasticsearchQuery);
		} else {
			searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
		}
		if (elasticsearchFilter != null) {
			searchRequestBuilder.setPostFilter(elasticsearchFilter);
		}
		searchRequestBuilder.setSize(0);
		return searchRequestBuilder.execute().actionGet().getHits().getTotalHits().value;
	}

	private <T> SearchRequestBuilder prepareCount(Query query, Class<T> clazz) {
		String indexName[] = !isEmpty(query.getIndices())
				? query.getIndices().toArray(new String[query.getIndices().size()])
				: retrieveIndexNameFromPersistentEntity(clazz);
		String types[] = !isEmpty(query.getTypes()) ? query.getTypes().toArray(new String[query.getTypes().size()])
				: retrieveTypeFromPersistentEntity(clazz);

		Assert.notNull(indexName, "No index defined for Query");

		SearchRequestBuilder countRequestBuilder = client.prepareSearch(indexName);

		if (types != null) {
			countRequestBuilder.setTypes(types);
		}
		countRequestBuilder.setSize(0);
		return countRequestBuilder;
	}

	

	private <T> MultiGetResponse getMultiResponse(Query searchQuery, Class<T> clazz) {

		String indexName = !isEmpty(searchQuery.getIndices()) ? searchQuery.getIndices().get(0)
				: getPersistentEntityFor(clazz).getIndexName();
		String type = !isEmpty(searchQuery.getTypes()) ? searchQuery.getTypes().get(0)
				: getPersistentEntityFor(clazz).getIndexType();

		Assert.notNull(indexName, "No index defined for Query");
		Assert.notNull(type, "No type define for Query");
		Assert.notEmpty(searchQuery.getIds(), "No Id define for Query");

		MultiGetRequestBuilder builder = client.prepareMultiGet();

		if (searchQuery.getFields() != null && !searchQuery.getFields().isEmpty()) {
			searchQuery.addSourceFilter(new FetchSourceFilter(toArray(searchQuery.getFields()), null));
		}

		for (String id : searchQuery.getIds()) {

			MultiGetRequest.Item item = new MultiGetRequest.Item(indexName, type, id);

			if (searchQuery.getRoute() != null) {
				item = item.routing(searchQuery.getRoute());
			}

			builder.add(item);
		}
		return builder.execute().actionGet();
	}


	private UpdateRequestBuilder prepareUpdate(UpdateQuery query) {

		String indexName = !StringUtils.isEmpty(query.getIndexName()) ? query.getIndexName()
				: getPersistentEntityFor(query.getClazz()).getIndexName();
		String type = !StringUtils.isEmpty(query.getType()) ? query.getType()
				: getPersistentEntityFor(query.getClazz()).getIndexType();

		Assert.notNull(indexName, "No index defined for Query");
		Assert.notNull(type, "No type define for Query");
		Assert.notNull(query.getId(), "No Id define for Query");
		Assert.notNull(query.getUpdateRequest(), "No UpdateRequest define for Query");

		UpdateRequest queryUpdateRequest = query.getUpdateRequest();

		UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(indexName, type, query.getId()) //
				.setRouting(queryUpdateRequest.routing()) //
				.setRetryOnConflict(queryUpdateRequest.retryOnConflict()) //
				.setTimeout(queryUpdateRequest.timeout()) //
				.setWaitForActiveShards(queryUpdateRequest.waitForActiveShards()) //
				.setRefreshPolicy(queryUpdateRequest.getRefreshPolicy()) //
				.setWaitForActiveShards(queryUpdateRequest.waitForActiveShards()) //
				.setScriptedUpsert(queryUpdateRequest.scriptedUpsert()) //
				.setDocAsUpsert(queryUpdateRequest.docAsUpsert());

		if (query.DoUpsert()) {
			updateRequestBuilder.setDocAsUpsert(true);
		}
		if (queryUpdateRequest.script() != null) {
			updateRequestBuilder.setScript(queryUpdateRequest.script());
		}
		if (queryUpdateRequest.doc() != null) {
			updateRequestBuilder.setDoc(queryUpdateRequest.doc());
		}
		if (queryUpdateRequest.upsertRequest() != null) {
			updateRequestBuilder.setUpsert(queryUpdateRequest.upsertRequest());
		}

		return updateRequestBuilder;
	}

//	@Override
//	public void bulkIndex(List<IndexQuery> queries, BulkOptions bulkOptions) {
//
//		Assert.notNull(queries, "List of IndexQuery must not be null");
//		Assert.notNull(bulkOptions, "BulkOptions must not be null");
//
//		BulkRequestBuilder bulkRequest = client.prepareBulk();
//		setBulkOptions(bulkRequest, bulkOptions);
//		for (IndexQuery query : queries) {
//			bulkRequest.add(prepareIndex(query));
//		}
//		checkForBulkUpdateFailure(bulkRequest.execute().actionGet());
//	}

	@Override
	public void bulkUpdate(List<UpdateQuery> queries, BulkOptions bulkOptions) {

		Assert.notNull(queries, "List of UpdateQuery must not be null");
		Assert.notNull(bulkOptions, "BulkOptions must not be null");

		BulkRequestBuilder bulkRequest = client.prepareBulk();
		setBulkOptions(bulkRequest, bulkOptions);
		for (UpdateQuery query : queries) {
			bulkRequest.add(prepareUpdate(query));
		}
		checkForBulkUpdateFailure(bulkRequest.execute().actionGet());
	}

	private static void setBulkOptions(BulkRequestBuilder bulkRequest, BulkOptions bulkOptions) {

		if (bulkOptions.getTimeout() != null) {
			bulkRequest.setTimeout(bulkOptions.getTimeout());
		}

		if (bulkOptions.getRefreshPolicy() != null) {
			bulkRequest.setRefreshPolicy(bulkOptions.getRefreshPolicy());
		}

		if (bulkOptions.getWaitForActiveShards() != null) {
			bulkRequest.setWaitForActiveShards(bulkOptions.getWaitForActiveShards());
		}

		if (bulkOptions.getPipeline() != null) {
			bulkRequest.pipeline(bulkOptions.getPipeline());
		}

		if (bulkOptions.getRoutingId() != null) {
			bulkRequest.routing(bulkOptions.getRoutingId());
		}
	}

	private void checkForBulkUpdateFailure(BulkResponse bulkResponse) {
		if (bulkResponse.hasFailures()) {
			Map<String, String> failedDocuments = new HashMap<>();
			for (BulkItemResponse item : bulkResponse.getItems()) {
				if (item.isFailed())
					failedDocuments.put(item.getId(), item.getFailureMessage());
			}
			throw new ElasticsearchException(
					"Bulk indexing has failures. Use ElasticsearchException.getFailedDocuments() for detailed messages ["
							+ failedDocuments + "]",
					failedDocuments);
		}
	}

	@Override
	public <T> boolean indexExists(Class<T> clazz) {
		return indexExists(getPersistentEntityFor(clazz).getIndexName());
	}

	@Override
	public boolean indexExists(String indexName) {
		return client.admin().indices().exists(indicesExistsRequest(indexName)).actionGet().isExists();
	}

	@Override
	public boolean typeExists(String index, String type) {
		return client.admin().cluster().prepareState().execute().actionGet().getState().metaData().index(index)
				.getMappings().containsKey(type);
	}

	@Override
	public <T> boolean deleteIndex(Class<T> clazz) {
		return deleteIndex(getPersistentEntityFor(clazz).getIndexName());
	}

	@Override
	public boolean deleteIndex(String indexName) {
		Assert.notNull(indexName, "No index defined for delete operation");
		if (indexExists(indexName)) {
			return client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet().isAcknowledged();
		}
		return false;
	}

	@Override
	public String delete(String indexName, String type, String id) {
		return client.prepareDelete(indexName, type, id).execute().actionGet().getId();
	}

	@Override
	public <T> String delete(Class<T> clazz, String id) {
		ElasticsearchPersistentEntity persistentEntity = getPersistentEntityFor(clazz);
		return delete(persistentEntity.getIndexName(), persistentEntity.getIndexType(), id);
	}

	@Override
	public <T> void delete(DeleteQuery deleteQuery, Class<T> clazz) {

		String indexName = !StringUtils.isEmpty(deleteQuery.getIndex()) ? deleteQuery.getIndex()
				: getPersistentEntityFor(clazz).getIndexName();
		String typeName = !StringUtils.isEmpty(deleteQuery.getType()) ? deleteQuery.getType()
				: getPersistentEntityFor(clazz).getIndexType();

		DeleteByQueryRequestBuilder requestBuilder = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE) //
				.source(indexName) //
				.filter(deleteQuery.getQuery()) //
				.abortOnVersionConflict(false) //
				.refresh(true);

		SearchRequestBuilder source = requestBuilder.source() //
				.setTypes(typeName);

		if (deleteQuery.getScrollTimeInMillis() != null)
			source.setScroll(TimeValue.timeValueMillis(deleteQuery.getScrollTimeInMillis()));

		requestBuilder.get();
	}


	private <T> SearchRequestBuilder prepareScroll(Query query, long scrollTimeInMillis, Class<T> clazz) {
		setPersistentEntityIndexAndType(query, clazz);
		return prepareScroll(query, scrollTimeInMillis, getPersistentEntity(clazz));
	}

	private SearchRequestBuilder prepareScroll(Query query, long scrollTimeInMillis,
                                               @Nullable ElasticsearchPersistentEntity<?> entity) {
		SearchRequestBuilder requestBuilder = client.prepareSearch(toArray(query.getIndices()))
				.setTypes(toArray(query.getTypes())).setScroll(TimeValue.timeValueMillis(scrollTimeInMillis)).setFrom(0)
				.setVersion(true);

		if (query.getPageable().isPaged()) {
			requestBuilder.setSize(query.getPageable().getPageSize());
		}

		if (query.getSourceFilter() != null) {
			SourceFilter sourceFilter = query.getSourceFilter();
			requestBuilder.setFetchSource(sourceFilter.getIncludes(), sourceFilter.getExcludes());
		}

		if (!isEmpty(query.getFields())) {
			requestBuilder.setFetchSource(toArray(query.getFields()), null);
		}

		if (query.getSort() != null) {
			prepareSort(query, requestBuilder, entity);
		}

		if (query.getIndicesOptions() != null) {
			requestBuilder.setIndicesOptions(query.getIndicesOptions());
		}

		if (query instanceof SearchQuery) {
			SearchQuery searchQuery = (SearchQuery) query;

			if (searchQuery.getHighlightFields() != null || searchQuery.getHighlightBuilder() != null) {
				HighlightBuilder highlightBuilder = searchQuery.getHighlightBuilder();
				if (highlightBuilder == null) {
					highlightBuilder = new HighlightBuilder();
				}
				if (searchQuery.getHighlightFields() != null) {
					for (HighlightBuilder.Field highlightField : searchQuery.getHighlightFields()) {
						highlightBuilder.field(highlightField);
					}
				}
				requestBuilder.highlighter(highlightBuilder);
			}
		}

		return requestBuilder;
	}


	private SearchResponse doScroll(SearchRequestBuilder requestBuilder, SearchQuery searchQuery) {
		Assert.notNull(searchQuery.getIndices(), "No index defined for Query");
		Assert.notNull(searchQuery.getTypes(), "No type define for Query");
		Assert.notNull(searchQuery.getPageable(), "Query.pageable is required for scan & scroll");

		if (searchQuery.getFilter() != null) {
			requestBuilder.setPostFilter(searchQuery.getFilter());
		}

		if (!isEmpty(searchQuery.getElasticsearchSorts())) {
			for (SortBuilder sort : searchQuery.getElasticsearchSorts()) {
				requestBuilder.addSort(sort);
			}
		}

		return getSearchResponse(requestBuilder.setQuery(searchQuery.getQuery()));
	}

	public <T> ScrolledPage<T> startScroll(long scrollTimeInMillis, SearchQuery searchQuery, Class<T> clazz) {
		SearchResponse response = doScroll(prepareScroll(searchQuery, scrollTimeInMillis, clazz), searchQuery);
		return resultsMapper.mapResults(response, clazz, null);
	}

	public <T> ScrolledPage<T> startScroll(long scrollTimeInMillis, CriteriaQuery criteriaQuery, Class<T> clazz) {
		SearchResponse response = doScroll(prepareScroll(criteriaQuery, scrollTimeInMillis, clazz), criteriaQuery);
		return resultsMapper.mapResults(response, clazz, null);
	}
	
	private SearchResponse doScroll(SearchRequestBuilder requestBuilder, CriteriaQuery criteriaQuery) {
		Assert.notNull(criteriaQuery.getIndices(), "No index defined for Query");
		Assert.notNull(criteriaQuery.getTypes(), "No type define for Query");
		Assert.notNull(criteriaQuery.getPageable(), "Query.pageable is required for scan & scroll");

		QueryBuilder elasticsearchQuery = new CriteriaQueryProcessor().createQueryFromCriteria(criteriaQuery.getCriteria());
		QueryBuilder elasticsearchFilter = new CriteriaFilterProcessor()
				.createFilterFromCriteria(criteriaQuery.getCriteria());

		if (elasticsearchQuery != null) {
			requestBuilder.setQuery(elasticsearchQuery);
		} else {
			requestBuilder.setQuery(QueryBuilders.matchAllQuery());
		}

		if (elasticsearchFilter != null) {
			requestBuilder.setPostFilter(elasticsearchFilter);
		}

		return getSearchResponse(requestBuilder);
	}

	

	public <T> ScrolledPage<T> startScroll(long scrollTimeInMillis, SearchQuery searchQuery, Class<T> clazz,
                                           SearchResultMapper mapper) {
		SearchResponse response = doScroll(prepareScroll(searchQuery, scrollTimeInMillis, clazz), searchQuery);
		return mapper.mapResults(response, clazz, null);
	}

	public <T> ScrolledPage<T> startScroll(long scrollTimeInMillis, CriteriaQuery criteriaQuery, Class<T> clazz,
                                           SearchResultMapper mapper) {
		SearchResponse response = doScroll(prepareScroll(criteriaQuery, scrollTimeInMillis, clazz), criteriaQuery);
		return mapper.mapResults(response, clazz, null);
	}

	public <T> ScrolledPage<T> continueScroll(@Nullable String scrollId, long scrollTimeInMillis, Class<T> clazz) {
		SearchResponse response = getSearchResponse(
				client.prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMillis(scrollTimeInMillis)).execute());
		return resultsMapper.mapResults(response, clazz, Pageable.unpaged());
	}

	public <T> ScrolledPage<T> continueScroll(@Nullable String scrollId, long scrollTimeInMillis, Class<T> clazz,
                                              SearchResultMapper mapper) {
		SearchResponse response = getSearchResponse(
				client.prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMillis(scrollTimeInMillis)).execute());
		return mapper.mapResults(response, clazz, Pageable.unpaged());
	}

	@Override
	public void clearScroll(String scrollId) {
		client.prepareClearScroll().addScrollId(scrollId).execute().actionGet();
	}

	@Override
	public <T> Page<T> moreLikeThis(MoreLikeThisQuery query, Class<T> clazz) {

		ElasticsearchPersistentEntity persistentEntity = getPersistentEntityFor(clazz);
		String indexName = !StringUtils.isEmpty(query.getIndexName()) ? query.getIndexName()
				: persistentEntity.getIndexName();
		String type = !StringUtils.isEmpty(query.getType()) ? query.getType() : persistentEntity.getIndexType();

		Assert.notNull(indexName, "No 'indexName' defined for MoreLikeThisQuery");
		Assert.notNull(type, "No 'type' defined for MoreLikeThisQuery");
		Assert.notNull(query.getId(), "No document id defined for MoreLikeThisQuery");

		MoreLikeThisQueryBuilder moreLikeThisQueryBuilder = moreLikeThisQuery(
				toArray(new MoreLikeThisQueryBuilder.Item(indexName, type, query.getId())));

		if (query.getMinTermFreq() != null) {
			moreLikeThisQueryBuilder.minTermFreq(query.getMinTermFreq());
		}
		if (query.getMaxQueryTerms() != null) {
			moreLikeThisQueryBuilder.maxQueryTerms(query.getMaxQueryTerms());
		}
		if (!isEmpty(query.getStopWords())) {
			moreLikeThisQueryBuilder.stopWords(toArray(query.getStopWords()));
		}
		if (query.getMinDocFreq() != null) {
			moreLikeThisQueryBuilder.minDocFreq(query.getMinDocFreq());
		}
		if (query.getMaxDocFreq() != null) {
			moreLikeThisQueryBuilder.maxDocFreq(query.getMaxDocFreq());
		}
		if (query.getMinWordLen() != null) {
			moreLikeThisQueryBuilder.minWordLength(query.getMinWordLen());
		}
		if (query.getMaxWordLen() != null) {
			moreLikeThisQueryBuilder.maxWordLength(query.getMaxWordLen());
		}
		if (query.getBoostTerms() != null) {
			moreLikeThisQueryBuilder.boostTerms(query.getBoostTerms());
		}

		return queryForPage(new NativeSearchQueryBuilder().withQuery(moreLikeThisQueryBuilder).build(), clazz);
	}

	private SearchResponse doSearch(SearchRequestBuilder searchRequest, SearchQuery searchQuery) {
		SearchRequestBuilder requestBuilder = prepareSearch(searchRequest, searchQuery);
		return getSearchResponse(requestBuilder);
	}

	private SearchRequestBuilder prepareSearch(SearchRequestBuilder searchRequest, SearchQuery searchQuery) {
		if (searchQuery.getFilter() != null) {
			searchRequest.setPostFilter(searchQuery.getFilter());
		}

		if (!isEmpty(searchQuery.getElasticsearchSorts())) {
			for (SortBuilder sort : searchQuery.getElasticsearchSorts()) {
				searchRequest.addSort(sort);
			}
		}

		if (!searchQuery.getScriptFields().isEmpty()) {
			// _source should be return all the time
			// searchRequest.addStoredField("_source");
			for (ScriptField scriptedField : searchQuery.getScriptFields()) {
				searchRequest.addScriptField(scriptedField.fieldName(), scriptedField.script());
			}
		}

		if (searchQuery.getCollapseBuilder() != null) {
			searchRequest.setCollapse(searchQuery.getCollapseBuilder());
		}

		if (searchQuery.getHighlightFields() != null || searchQuery.getHighlightBuilder() != null) {
			HighlightBuilder highlightBuilder = searchQuery.getHighlightBuilder();
			if (highlightBuilder == null) {
				highlightBuilder = new HighlightBuilder();
			}
			if (searchQuery.getHighlightFields() != null) {
				for (HighlightBuilder.Field highlightField : searchQuery.getHighlightFields()) {
					highlightBuilder.field(highlightField);
				}
			}
			searchRequest.highlighter(highlightBuilder);
		}

		if (!isEmpty(searchQuery.getIndicesBoost())) {
			for (IndexBoost indexBoost : searchQuery.getIndicesBoost()) {
				searchRequest.addIndexBoost(indexBoost.getIndexName(), indexBoost.getBoost());
			}
		}

		if (!isEmpty(searchQuery.getAggregations())) {
			for (AbstractAggregationBuilder aggregationBuilder : searchQuery.getAggregations()) {
				searchRequest.addAggregation(aggregationBuilder);
			}
		}

		if (!isEmpty(searchQuery.getFacets())) {
			for (FacetRequest aggregatedFacet : searchQuery.getFacets()) {
				searchRequest.addAggregation(aggregatedFacet.getFacet());
			}
		}

		return searchRequest.setQuery(searchQuery.getQuery());
	}

	private SearchResponse getSearchResponse(SearchRequestBuilder requestBuilder) {

		if (QUERY_LOGGER.isDebugEnabled()) {
			QUERY_LOGGER.debug(requestBuilder.toString());
		}

		return getSearchResponse(requestBuilder.execute());
	}

	private SearchResponse getSearchResponse(ActionFuture<SearchResponse> response) {
		return searchTimeout == null ? response.actionGet() : response.actionGet(searchTimeout);
	}

//	private <T> boolean createIndexIfNotCreated(Class<T> clazz) {
//		return indexExists(getPersistentEntityFor(clazz).getIndexName()) || createIndexWithSettings(clazz);
//	}

//	private <T> boolean createIndexWithSettings(Class<T> clazz) {
//		if (clazz.isAnnotationPresent(Setting.class)) {
//			String settingPath = clazz.getAnnotation(Setting.class).settingPath();
//			if (!StringUtils.isEmpty(settingPath)) {
//				String settings = ResourceUtil.readFileFromClasspath(settingPath);
//				if (!StringUtils.isEmpty(settings)) {
//					return createIndex(getPersistentEntityFor(clazz).getIndexName(), settings);
//				}
//			} else {
//				LOGGER.info("settingPath in @Setting has to be defined. Using default instead.");
//			}
//		}
//		return createIndex(getPersistentEntityFor(clazz).getIndexName(), getDefaultSettings(getPersistentEntityFor(clazz)));
//	}

	@Override
	public boolean createIndex(String indexName, Object settings) {
		CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
		if (settings instanceof String) {
			createIndexRequestBuilder.setSettings(String.valueOf(settings), Requests.INDEX_CONTENT_TYPE);
		} else if (settings instanceof Map) {
			createIndexRequestBuilder.setSettings((Map) settings);
		} else if (settings instanceof XContentBuilder) {
			createIndexRequestBuilder.setSettings((XContentBuilder) settings);
		}
		return createIndexRequestBuilder.execute().actionGet().isAcknowledged();
	}

	@Override
	public <T> boolean createIndex(Class<T> clazz, Object settings) {
		return createIndex(getPersistentEntityFor(clazz).getIndexName(), settings);
	}

	private <T> Map<String, String> getDefaultSettings(ElasticsearchPersistentEntity<T> persistentEntity) {

		if (persistentEntity.isUseServerConfiguration())
			return new HashMap<>();

		return new MapBuilder<String, String>().put("index.number_of_shards", String.valueOf(persistentEntity.getShards()))
				.put("index.number_of_replicas", String.valueOf(persistentEntity.getReplicas()))
				.put("index.refresh_interval", persistentEntity.getRefreshInterval())
				.put("index.store.type", persistentEntity.getIndexStoreType()).map();
	}

	@Override
	public <T> Map<String, Object> getSetting(Class<T> clazz) {
		return getSetting(getPersistentEntityFor(clazz).getIndexName());
	}

	@Override
	public Map<String, Object> getSetting(String indexName) {
		Assert.notNull(indexName, "No index defined for getSettings");
		Settings settings = client.admin().indices().getSettings(new GetSettingsRequest()).actionGet().getIndexToSettings()
				.get(indexName);
		return settings.keySet().stream().collect(Collectors.toMap((key) -> key, (key) -> settings.get(key)));
	}

	private <T> SearchRequestBuilder prepareSearch(Query query, Class<T> clazz) {
		setPersistentEntityIndexAndType(query, clazz);
		return prepareSearch(query, getPersistentEntity(clazz));
	}

	private SearchRequestBuilder prepareSearch(Query query, @Nullable ElasticsearchPersistentEntity<?> entity) {
		Assert.notNull(query.getIndices(), "No index defined for Query");
		Assert.notNull(query.getTypes(), "No type defined for Query");

		int startRecord = 0;
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(toArray(query.getIndices()))
				.setSearchType(query.getSearchType()).setTypes(toArray(query.getTypes())).setVersion(true)
				.setTrackScores(query.getTrackScores());

		if (query.getSourceFilter() != null) {
			SourceFilter sourceFilter = query.getSourceFilter();
			searchRequestBuilder.setFetchSource(sourceFilter.getIncludes(), sourceFilter.getExcludes());
		}

		if (query.getPageable().isPaged()) {
			long offset = query.getPageable().getOffset();

			if (offset > Integer.MAX_VALUE) {
				throw new IllegalArgumentException(String.format("Offset must not be more than %d", Integer.MAX_VALUE));
			}

			startRecord = (int) offset;
			searchRequestBuilder.setSize(query.getPageable().getPageSize());
		} else {
			startRecord = 0;
			searchRequestBuilder.setSize(10_000);
		}
		searchRequestBuilder.setFrom(startRecord);

		if (!query.getFields().isEmpty()) {
			searchRequestBuilder.setFetchSource(toArray(query.getFields()), null);
		}

		if (query.getIndicesOptions() != null) {
			searchRequestBuilder.setIndicesOptions(query.getIndicesOptions());
		}

		if (query.getSort() != null) {
			prepareSort(query, searchRequestBuilder, entity);
		}

		if (query.getMinScore() > 0) {
			searchRequestBuilder.setMinScore(query.getMinScore());
		}

		if (query.getPreference() != null) {
			searchRequestBuilder.setPreference(query.getPreference());
		}

		return searchRequestBuilder;
	}

	private void prepareSort(Query query, SearchRequestBuilder searchRequestBuilder,
                             @Nullable ElasticsearchPersistentEntity<?> entity) {
		for (Sort.Order order : query.getSort()) {
			SortOrder sortOrder = order.getDirection().isDescending() ? SortOrder.DESC : SortOrder.ASC;

			if (ScoreSortBuilder.NAME.equals(order.getProperty())) {
				ScoreSortBuilder sort = SortBuilders //
						.scoreSort() //
						.order(sortOrder);

				searchRequestBuilder.addSort(sort);
			} else {
				ElasticsearchPersistentProperty property = entity != null //
						? entity.getPersistentProperty(order.getProperty()) //
						: null;
				String fieldName = property != null ? property.getFieldName() : order.getProperty();
				FieldSortBuilder sort = SortBuilders //
						.fieldSort(fieldName) //
						.order(sortOrder);

				if (order.getNullHandling() == Sort.NullHandling.NULLS_FIRST) {
					sort.missing("_first");
				} else if (order.getNullHandling() == Sort.NullHandling.NULLS_LAST) {
					sort.missing("_last");
				}

				searchRequestBuilder.addSort(sort);
			}
		}
	}

//	private IndexRequestBuilder prepareIndex(IndexQuery query) {
//		try {
//			String indexName = StringUtils.isEmpty(query.getIndexName())
//					? retrieveIndexNameFromPersistentEntity(query.getObject().getClass())[0]
//					: query.getIndexName();
//			String type = StringUtils.isEmpty(query.getType())
//					? retrieveTypeFromPersistentEntity(query.getObject().getClass())[0]
//					: query.getType();
//
//			IndexRequestBuilder indexRequestBuilder = null;
//
//			if (query.getObject() != null) {
//				String id = StringUtils.isEmpty(query.getId()) ? getPersistentEntityId(query.getObject()) : query.getId();
//				// If we have a query id and a document id, do not ask ES to generate one.
//				if (id != null) {
//					indexRequestBuilder = client.prepareIndex(indexName, type, id);
//				} else {
//					indexRequestBuilder = client.prepareIndex(indexName, type);
//				}
//				indexRequestBuilder.setSource(resultsMapper.getEntityMapper().mapToString(query.getObject()),
//						Requests.INDEX_CONTENT_TYPE);
//			} else if (query.getSource() != null) {
//				indexRequestBuilder = client.prepareIndex(indexName, type, query.getId()).setSource(query.getSource(),
//						Requests.INDEX_CONTENT_TYPE);
//			} else {
//				throw new ElasticsearchException(
//						"object or source is null, failed to index the document [id: " + query.getId() + "]");
//			}
//			if (query.getVersion() != null) {
//				indexRequestBuilder.setVersion(query.getVersion());
//				VersionType versionType = retrieveVersionTypeFromPersistentEntity(query.getObject().getClass());
//				indexRequestBuilder.setVersionType(versionType);
//			}
//
//			if (query.getParentId() != null) {
//				indexRequestBuilder.setParent(query.getParentId());
//			}
//
//			return indexRequestBuilder;
//		} catch (IOException e) {
//			throw new ElasticsearchException("failed to index the document [id: " + query.getId() + "]", e);
//		}
//	}

	@Override
	public void refresh(String indexName) {
		Assert.notNull(indexName, "No index defined for refresh()");
		client.admin().indices().refresh(refreshRequest(indexName)).actionGet();
	}

	@Override
	public <T> void refresh(Class<T> clazz) {
		refresh(getPersistentEntityFor(clazz).getIndexName());
	}

	@Override
	public List<AliasMetaData> queryForAlias(String indexName) {
		return client.admin().indices().getAliases(new GetAliasesRequest().indices(indexName)).actionGet().getAliases()
				.get(indexName);
	}

	@Nullable
	private ElasticsearchPersistentEntity<?> getPersistentEntity(@Nullable Class<?> clazz) {
		return clazz != null ? elasticsearchConverter.getMappingContext().getPersistentEntity(clazz) : null;
	}

	@Override
	public ElasticsearchPersistentEntity getPersistentEntityFor(Class clazz) {
		Assert.isTrue(clazz.isAnnotationPresent(Document.class), "Unable to identify index name. " + clazz.getSimpleName()
				+ " is not a Document. Make sure the document class is annotated with @Document(indexName=\"foo\")");
		return elasticsearchConverter.getMappingContext().getRequiredPersistentEntity(clazz);
	}

	private String getPersistentEntityId(Object entity) {

		ElasticsearchPersistentEntity<?> persistentEntity = getPersistentEntityFor(entity.getClass());
		Object identifier = persistentEntity.getIdentifierAccessor(entity).getIdentifier();

		if (identifier != null) {
			return identifier.toString();
		}

		return null;
	}

	private void setPersistentEntityId(Object entity, String id) {

		ElasticsearchPersistentEntity<?> persistentEntity = getPersistentEntityFor(entity.getClass());
		ElasticsearchPersistentProperty idProperty = persistentEntity.getIdProperty();

		// Only deal with text because ES generated Ids are strings !

		if (idProperty != null && idProperty.getType().isAssignableFrom(String.class)) {
			persistentEntity.getPropertyAccessor(entity).setProperty(idProperty, id);
		}
	}

	private void setPersistentEntityIndexAndType(Query query, Class clazz) {
		if (query.getIndices().isEmpty()) {
			query.addIndices(retrieveIndexNameFromPersistentEntity(clazz));
		}
		if (query.getTypes().isEmpty()) {
			query.addTypes(retrieveTypeFromPersistentEntity(clazz));
		}
	}

	private String[] retrieveIndexNameFromPersistentEntity(Class clazz) {
		if (clazz != null) {
			return new String[] { getPersistentEntityFor(clazz).getIndexName() };
		}
		return null;
	}

	private String[] retrieveTypeFromPersistentEntity(Class clazz) {
		if (clazz != null) {
			return new String[] { getPersistentEntityFor(clazz).getIndexType() };
		}
		return null;
	}

	private VersionType retrieveVersionTypeFromPersistentEntity(Class clazz) {
		if (clazz != null) {
			return getPersistentEntityFor(clazz).getVersionType();
		}
		return VersionType.EXTERNAL;
	}

	private List<String> extractIds(SearchResponse response) {
		List<String> ids = new ArrayList<>();
		for (SearchHit hit : response.getHits()) {
			if (hit != null) {
				ids.add(hit.getId());
			}
		}
		return ids;
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		if (elasticsearchConverter instanceof ApplicationContextAware) {
			((ApplicationContextAware) elasticsearchConverter).setApplicationContext(context);
		}
	}

	private static String[] toArray(List<String> values) {
		String[] valuesAsArray = new String[values.size()];
		return values.toArray(valuesAsArray);
	}

	private static MoreLikeThisQueryBuilder.Item[] toArray(MoreLikeThisQueryBuilder.Item... values) {
		return values;
	}

	protected ResultsMapper getResultsMapper() {
		return resultsMapper;
	}

	public SearchResponse suggest(SuggestBuilder suggestion, String... indices) {
		return client.prepareSearch(indices).suggest(suggestion).get();
	}

	public SearchResponse suggest(SuggestBuilder suggestion, Class clazz) {
		return suggest(suggestion, retrieveIndexNameFromPersistentEntity(clazz));
	}

}

class CriteriaQueryProcessor {


	QueryBuilder createQueryFromCriteria(Criteria criteria) {
		if (criteria == null)
			return null;

		List<QueryBuilder> shouldQueryBuilderList = new LinkedList<>();
		List<QueryBuilder> mustNotQueryBuilderList = new LinkedList<>();
		List<QueryBuilder> mustQueryBuilderList = new LinkedList<>();

		ListIterator<Criteria> chainIterator = criteria.getCriteriaChain().listIterator();

		QueryBuilder firstQuery = null;
		boolean negateFirstQuery = false;

		while (chainIterator.hasNext()) {
			Criteria chainedCriteria = chainIterator.next();
			QueryBuilder queryFragmentForCriteria = createQueryFragmentForCriteria(chainedCriteria);
			if (queryFragmentForCriteria != null) {
				if (firstQuery == null) {
					firstQuery = queryFragmentForCriteria;
					negateFirstQuery = chainedCriteria.isNegating();
					continue;
				}
				if (chainedCriteria.isOr()) {
					shouldQueryBuilderList.add(queryFragmentForCriteria);
				} else if (chainedCriteria.isNegating()) {
					mustNotQueryBuilderList.add(queryFragmentForCriteria);
				} else {
					mustQueryBuilderList.add(queryFragmentForCriteria);
				}
			}
		}

		if (firstQuery != null) {
			if (!shouldQueryBuilderList.isEmpty() && mustNotQueryBuilderList.isEmpty() && mustQueryBuilderList.isEmpty()) {
				shouldQueryBuilderList.add(0, firstQuery);
			} else {
				if (negateFirstQuery) {
					mustNotQueryBuilderList.add(0, firstQuery);
				} else {
					mustQueryBuilderList.add(0, firstQuery);
				}
			}
		}

		BoolQueryBuilder query = null;

		if (!shouldQueryBuilderList.isEmpty() || !mustNotQueryBuilderList.isEmpty() || !mustQueryBuilderList.isEmpty()) {

			query = boolQuery();

			for (QueryBuilder qb : shouldQueryBuilderList) {
				query.should(qb);
			}
			for (QueryBuilder qb : mustNotQueryBuilderList) {
				query.mustNot(qb);
			}
			for (QueryBuilder qb : mustQueryBuilderList) {
				query.must(qb);
			}
		}

		return query;
	}


	private QueryBuilder createQueryFragmentForCriteria(Criteria chainedCriteria) {
		if (chainedCriteria.getQueryCriteriaEntries().isEmpty())
			return null;

		Iterator<Criteria.CriteriaEntry> it = chainedCriteria.getQueryCriteriaEntries().iterator();
		boolean singeEntryCriteria = (chainedCriteria.getQueryCriteriaEntries().size() == 1);

		String fieldName = chainedCriteria.getField().getName();
		Assert.notNull(fieldName, "Unknown field");
		QueryBuilder query = null;

		if (singeEntryCriteria) {
			Criteria.CriteriaEntry entry = it.next();
			query = processCriteriaEntry(entry, fieldName);
		} else {
			query = boolQuery();
			while (it.hasNext()) {
				Criteria.CriteriaEntry entry = it.next();
				((BoolQueryBuilder) query).must(processCriteriaEntry(entry, fieldName));
			}
		}

		addBoost(query, chainedCriteria.getBoost());
		return query;
	}


	private QueryBuilder processCriteriaEntry(Criteria.CriteriaEntry entry,/* OperationKey key, Object value,*/ String fieldName) {
		Object value = entry.getValue();
		if (value == null) {
			return null;
		}
		OperationKey key = entry.getKey();
		QueryBuilder query = null;

		String searchText = org.apache.lucene.queryparser.flexible.core.util.StringUtils.toString(value);

		Iterable<Object> collection = null;

		switch (key) {
			case EQUALS:
				query = queryStringQuery(searchText).field(fieldName).defaultOperator(AND);
				break;
			case CONTAINS:
				query = queryStringQuery("*" + searchText + "*").field(fieldName).analyzeWildcard(true);
				break;
			case STARTS_WITH:
				query = queryStringQuery(searchText + "*").field(fieldName).analyzeWildcard(true);
				break;
			case ENDS_WITH:
				query = queryStringQuery("*" + searchText).field(fieldName).analyzeWildcard(true);
				break;
			case EXPRESSION:
				query = queryStringQuery(searchText).field(fieldName);
				break;
			case LESS_EQUAL:
				query = rangeQuery(fieldName).lte(value);
				break;
			case GREATER_EQUAL:
				query = rangeQuery(fieldName).gte(value);
				break;
			case BETWEEN:
				Object[] ranges = (Object[]) value;
				query = rangeQuery(fieldName).from(ranges[0]).to(ranges[1]);
				break;
			case LESS:
				query = rangeQuery(fieldName).lt(value);
				break;
			case GREATER:
				query = rangeQuery(fieldName).gt(value);
				break;
			case FUZZY:
				query = fuzzyQuery(fieldName, searchText);
				break;
			case IN:
				query = boolQuery();
				collection = (Iterable<Object>) value;
				for (Object item : collection) {
					((BoolQueryBuilder) query).should(queryStringQuery(item.toString()).field(fieldName));
				}
				break;
			case NOT_IN:
				query = boolQuery();
				collection = (Iterable<Object>) value;
				for (Object item : collection) {
					((BoolQueryBuilder) query).mustNot(queryStringQuery(item.toString()).field(fieldName));
				}
				break;
		}
		return query;
	}

	private void addBoost(QueryBuilder query, float boost) {
		if (Float.isNaN(boost)) {
			return;
		}
		query.boost(boost);
	}
}

class CriteriaFilterProcessor {


	QueryBuilder createFilterFromCriteria(Criteria criteria) {
		List<QueryBuilder> fbList = new LinkedList<>();
		QueryBuilder filter = null;

		ListIterator<Criteria> chainIterator = criteria.getCriteriaChain().listIterator();

		while (chainIterator.hasNext()) {
			QueryBuilder fb = null;
			Criteria chainedCriteria = chainIterator.next();
			if (chainedCriteria.isOr()) {
				fb = QueryBuilders.boolQuery();
				for(QueryBuilder f: createFilterFragmentForCriteria(chainedCriteria)){
					((BoolQueryBuilder)fb).should(f);
				}
				fbList.add(fb);
			} else if (chainedCriteria.isNegating()) {
				List<QueryBuilder> negationFilters = buildNegationFilter(criteria.getField().getName(), criteria.getFilterCriteriaEntries().iterator());

				if (!negationFilters.isEmpty()) {
					fbList.addAll(negationFilters);
				}
			} else {
				fbList.addAll(createFilterFragmentForCriteria(chainedCriteria));
			}
		}

		if (!fbList.isEmpty()) {
			if (fbList.size() == 1) {
				filter = fbList.get(0);
			} else {
				filter = QueryBuilders.boolQuery();
				for(QueryBuilder f: fbList) {
					((BoolQueryBuilder)filter).must(f);
				}
			}
		}
		return filter;
	}


	private List<QueryBuilder> createFilterFragmentForCriteria(Criteria chainedCriteria) {
		Iterator<Criteria.CriteriaEntry> it = chainedCriteria.getFilterCriteriaEntries().iterator();
		List<QueryBuilder> filterList = new LinkedList<>();

		String fieldName = chainedCriteria.getField().getName();
		Assert.notNull(fieldName, "Unknown field");
		QueryBuilder filter = null;

		while (it.hasNext()) {
			Criteria.CriteriaEntry entry = it.next();
			filter = processCriteriaEntry(entry.getKey(), entry.getValue(), fieldName);
			filterList.add(filter);
		}

		return filterList;
	}


	private QueryBuilder processCriteriaEntry(OperationKey key, Object value, String fieldName) {
		if (value == null) {
			return null;
		}
		QueryBuilder filter = null;

		switch (key) {
			case WITHIN: {
				GeoDistanceQueryBuilder geoDistanceQueryBuilder = QueryBuilders.geoDistanceQuery(fieldName);

				Assert.isTrue(value instanceof Object[], "Value of a geo distance filter should be an array of two values.");
				Object[] valArray = (Object[]) value;
				Assert.noNullElements(valArray, "Geo distance filter takes 2 not null elements array as parameter.");
				Assert.isTrue(valArray.length == 2, "Geo distance filter takes a 2-elements array as parameter.");
				Assert.isTrue(valArray[0] instanceof GeoPoint || valArray[0] instanceof String || valArray[0] instanceof Point, "First element of a geo distance filter must be a GeoPoint, a Point or a text");
				Assert.isTrue(valArray[1] instanceof String || valArray[1] instanceof Distance, "Second element of a geo distance filter must be a text or a Distance");

				StringBuilder dist = new StringBuilder();

				if (valArray[1] instanceof Distance) {
					extractDistanceString((Distance) valArray[1], dist);
				} else {
					dist.append((String) valArray[1]);
				}

				if (valArray[0] instanceof GeoPoint) {
					GeoPoint loc = (GeoPoint) valArray[0];
					geoDistanceQueryBuilder.point(loc.getLat(),loc.getLon()).distance(dist.toString()).geoDistance(GeoDistance.PLANE);
				} else if (valArray[0] instanceof Point) {
					GeoPoint loc = GeoPoint.fromPoint((Point) valArray[0]);
					geoDistanceQueryBuilder.point(loc.getLat(), loc.getLon()).distance(dist.toString()).geoDistance(GeoDistance.PLANE);
				} else {
					String loc = (String) valArray[0];
					if (loc.contains(",")) {
						String c[] = loc.split(",");
						geoDistanceQueryBuilder.point(Double.parseDouble(c[0]), Double.parseDouble(c[1])).distance(dist.toString()).geoDistance(GeoDistance.PLANE);
					} else {
						geoDistanceQueryBuilder.geohash(loc).distance(dist.toString()).geoDistance(GeoDistance.PLANE);
					}
				}
				filter = geoDistanceQueryBuilder;

				break;
			}

			case BBOX: {
				filter = QueryBuilders.geoBoundingBoxQuery(fieldName);

				Assert.isTrue(value instanceof Object[], "Value of a boundedBy filter should be an array of one or two values.");
				Object[] valArray = (Object[]) value;
				Assert.noNullElements(valArray, "Geo boundedBy filter takes a not null element array as parameter.");

				if (valArray.length == 1) {
					//GeoEnvelop
					oneParameterBBox((GeoBoundingBoxQueryBuilder) filter, valArray[0]);
				} else if (valArray.length == 2) {
					//2x GeoPoint
					//2x text
					twoParameterBBox((GeoBoundingBoxQueryBuilder) filter, valArray);
				} else {
					//error
					Assert.isTrue(false, "Geo distance filter takes a 1-elements array(GeoBox) or 2-elements array(GeoPoints or Strings(format lat,lon or geohash)).");
				}
				break;
			}
		}

		return filter;
	}


	/**
	 * extract the distance string from a {@link org.springframework.data.geo.Distance} object.
	 *
	 * @param distance distance object to extract string from
	 * @param sb StringBuilder to build the distance string
	 */
	private void extractDistanceString(Distance distance, StringBuilder sb) {
		// handle Distance object
		sb.append((int) distance.getValue());

		Metrics metric = (Metrics) distance.getMetric();

		switch (metric) {
			case KILOMETERS:
				sb.append("km");
				break;
			case MILES:
				sb.append("mi");
				break;
		}
	}

	private void oneParameterBBox(GeoBoundingBoxQueryBuilder filter, Object value) {
		Assert.isTrue(value instanceof GeoBox || value instanceof Box, "single-element of boundedBy filter must be type of GeoBox or Box");

		GeoBox geoBBox;
		if (value instanceof Box) {
			Box sdbox = (Box) value;
			geoBBox = GeoBox.fromBox(sdbox);
		} else {
			geoBBox = (GeoBox) value;
		}

		filter.setCorners(geoBBox.getTopLeft().getLat(), geoBBox.getTopLeft().getLon(), geoBBox.getBottomRight().getLat(), geoBBox.getBottomRight().getLon());
	}

	private static boolean isType(Object[] array, Class clazz) {
		for (Object o : array) {
			if (!clazz.isInstance(o)) {
				return false;
			}
		}
		return true;
	}

	private void twoParameterBBox(GeoBoundingBoxQueryBuilder filter, Object[] values) {
		Assert.isTrue(isType(values, GeoPoint.class) || isType(values, String.class), " both elements of boundedBy filter must be type of GeoPoint or text(format lat,lon or geohash)");
		if (values[0] instanceof GeoPoint) {
			GeoPoint topLeft = (GeoPoint) values[0];
			GeoPoint bottomRight = (GeoPoint) values[1];
			filter.setCorners(topLeft.getLat(), topLeft.getLon(), bottomRight.getLat(), bottomRight.getLon());
		} else {
			String topLeft = (String) values[0];
			String bottomRight = (String) values[1];
			filter.setCorners(topLeft, bottomRight);
		}
	}

	private List<QueryBuilder> buildNegationFilter(String fieldName, Iterator<Criteria.CriteriaEntry> it) {
		List<QueryBuilder> notFilterList = new LinkedList<>();

		while (it.hasNext()) {
			Criteria.CriteriaEntry criteriaEntry = it.next();
			QueryBuilder notFilter = QueryBuilders.boolQuery().mustNot(processCriteriaEntry(criteriaEntry.getKey(), criteriaEntry.getValue(), fieldName));
			notFilterList.add(notFilter);
		}

		return notFilterList;
	}
}
