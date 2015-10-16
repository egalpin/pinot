package com.linkedin.thirdeye.dashboard.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.ws.rs.core.MultivaluedMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.dashboard.api.DimensionGroupSpec;
import com.linkedin.thirdeye.dashboard.api.FunnelHeatMapRow;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.api.funnel.CustomFunnelSpec;
import com.linkedin.thirdeye.dashboard.api.funnel.FunnelSpec;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;
import com.linkedin.thirdeye.dashboard.views.FunnelHeatMapView;


public class FunnelsDataProvider {
  private static String FUNNELS_CONFIG_FILE_NAME = "funnels.yml";

  private static final Logger LOG = LoggerFactory.getLogger(FunnelsDataProvider.class);
  private static final Joiner METRIC_FUNCTION_JOINER = Joiner.on(",");
  private static final TypeReference<List<String>> LIST_REF = new TypeReference<List<String>>() {
  };
  private final File funnelsRoot;
  private final String serverUri;
  private final QueryCache queryCache;
  private final Map<String, CustomFunnelSpec> funnelSpecsMap;

  public FunnelsDataProvider(File funnelsRoot, String serverUri, QueryCache queryCache, DataCache dataCache)
      throws JsonProcessingException {
    this.funnelsRoot = funnelsRoot;
    this.serverUri = serverUri;
    this.queryCache = queryCache;
    this.funnelSpecsMap = new HashMap<String, CustomFunnelSpec>();
    loadConfigs();
    LOG.info("loaded custom funnel configs with {} ", new ObjectMapper().writeValueAsString(funnelSpecsMap));
  }

  public CustomFunnelSpec getFunnelSpecFor(String collection) {
    return funnelSpecsMap.get(collection);
  }

  public List<FunnelHeatMapView> computeFunnelViews(String collection, String selectedFunnels, DateTime currentDateTime,
      MultivaluedMap<String, String> dimensionValues) throws Exception {
    String[] funnels = selectedFunnels.split(",");
    if (funnels.length == 0) {
      return null;
    }

    List<FunnelHeatMapView> funnelViews = new ArrayList<FunnelHeatMapView>();

    for (String funnel : funnels) {
      LOG.info("adding funnel views for collection, {}, with funnel name {}", collection, funnel);
      funnelViews.add(getFunnelDataFor(collection, funnel, currentDateTime.getYear(), currentDateTime.getMonthOfYear(),
          currentDateTime.getDayOfMonth(), dimensionValues));
    }

    return funnelViews;
  }

  public List<String> getFunnelNamesFor(String collection) {
    List<String> funnelNames = new ArrayList<String>();

    if (!funnelSpecsMap.containsKey(collection)) {
      return funnelNames;
    }

    for (FunnelSpec spec : funnelSpecsMap.get(collection).getFunnels().values()) {
      funnelNames.add(spec.getName());
    }

    return funnelNames;
  }

  // currently funnels will overlook the current granularity and baseline granularity
  // it will only present views for every hour within the 24 hour period
  // filter format will be dimName1:dimValue1;dimName2:dimValue2

  public FunnelHeatMapView getFunnelDataFor(String collection, String funnel, Integer year, Integer month, Integer day,
      MultivaluedMap<String, String> dimensionValuesMap) throws Exception {

    // TODO : {dpatel} : this entire flow is extremely similar to custom dashboards, we should merge them

    FunnelSpec spec = funnelSpecsMap.get(collection).getFunnels().get(funnel);

    // get current start and end time
    DateTime currentEnd = new DateTime(year, month, day, 0, 0);
    DateTime currentStart = currentEnd.minusDays(1);

    // get baseline start and end
    DateTime baselineEnd = currentEnd.minusDays(7);
    DateTime baselineStart = baselineEnd.minusDays(1);

    String metricFunction = "AGGREGATE_1_HOURS(" + METRIC_FUNCTION_JOINER.join(spec.getActualMetricNames()) + ")";

    DimensionGroupSpec dimSpec = DimensionGroupSpec.emptySpec(collection);

    Map<String, Map<String, List<String>>> dimensionGroups =
        DimensionGroupSpec.emptySpec(collection).getReverseMapping();

    String baselineSql =
        SqlUtils.getSql(metricFunction, collection, baselineStart, baselineEnd, dimensionValuesMap, dimensionGroups);
    String currentSql =
        SqlUtils.getSql(metricFunction, collection, currentStart, currentEnd, dimensionValuesMap, dimensionGroups);

    LOG.info("funnel queries for collection : {}, with name : {} ", collection, spec.getName());
    LOG.info("Generated SQL: {}", baselineSql);
    LOG.info("Generated SQL: {}", currentSql);

    // Query
    Future<QueryResult> baselineResult = queryCache.getQueryResultAsync(serverUri, baselineSql);
    Future<QueryResult> currentResult = queryCache.getQueryResultAsync(serverUri, currentSql);

    // Baseline data
    Map<Long, Number[]> baselineData = CustomDashboardResource.extractFunnelData(baselineResult.get());
    Map<Long, Number[]> currentData = CustomDashboardResource.extractFunnelData(currentResult.get());

    // Compose result
    List<FunnelHeatMapRow> table = new ArrayList<FunnelHeatMapRow>();
    DateTime currentCursor = new DateTime(currentStart.getMillis());
    DateTime baselineCursor = new DateTime(baselineStart.getMillis());
    while (currentCursor.compareTo(currentEnd) < 0 && baselineCursor.compareTo(baselineEnd) < 0) {
      // Get values for this time
      Number[] baselineValues = baselineData.get(baselineCursor.getMillis());
      Number[] currentValues = currentData.get(currentCursor.getMillis());

      FunnelHeatMapRow row = new FunnelHeatMapRow(new DateTime(baselineCursor).toDateTime(DateTimeZone.UTC),
          baselineValues, new DateTime(currentCursor).toDateTime(DateTimeZone.UTC), currentValues);
      table.add(row);

      // Increment
      currentCursor = currentCursor.plusHours(1);
      baselineCursor = baselineCursor.plusHours(1);
    }

    // Get mapping of metric name to index
    Map<String, Integer> metricNameToIndex = new HashMap<>();
    List<String> resultMetrics = baselineResult.get().getMetrics();
    for (int i = 0; i < resultMetrics.size(); i++) {
      metricNameToIndex.put(resultMetrics.get(i), i);
    }

    // Filter (since query result set will contain primitive metrics for each derived one)
    List<FunnelHeatMapRow> filteredTable = new ArrayList<>();
    int metricCount = spec.getActualMetricNames().size();
    List<FunnelHeatMapRow> filteredCumulativeTable = new ArrayList<>();
    Number[] cumulativeBaseline = new Number[metricCount];
    Arrays.fill(cumulativeBaseline, 0.0);
    Number[] cumulativeCurrent = new Number[metricCount];
    Arrays.fill(cumulativeCurrent, 0.0);

    for (FunnelHeatMapRow row : table) {
      Number[] filteredBaseline = new Number[metricCount];
      Number[] filteredCurrent = new Number[metricCount];
      for (int i = 0; i < metricCount; i++) {
        String metricName = spec.getActualMetricNames().get(i);
        Integer metricIdx = metricNameToIndex.get(metricName);

        Number baselineValue = null;
        if (row.getBaseline() != null) {
          try {
            baselineValue = row.getBaseline()[metricIdx];
          } catch (Exception e) {
            LOG.error("", e);
          }
        }
        filteredBaseline[i] = baselineValue;

        Number currentValue = null;
        if (row.getCurrent() != null) {
          try {
            currentValue = row.getCurrent()[metricIdx];
          } catch (Exception e) {
            LOG.error("", e);
          }
        }
        filteredCurrent[i] = currentValue;

        cumulativeBaseline[i] =
            cumulativeBaseline[i].doubleValue() + (baselineValue == null ? 0.0 : baselineValue.doubleValue());
        cumulativeCurrent[i] =
            cumulativeCurrent[i].doubleValue() + (currentValue == null ? 0.0 : currentValue.doubleValue());
      }

      FunnelHeatMapRow filteredRow =
          new FunnelHeatMapRow(row.getBaselineTime(), filteredBaseline, row.getCurrentTime(), filteredCurrent);
      filteredTable.add(filteredRow);

      Number[] cumulativeBaselineCopy = Arrays.copyOf(cumulativeBaseline, cumulativeBaseline.length);
      Number[] cumulativeCurrentCopy = Arrays.copyOf(cumulativeCurrent, cumulativeCurrent.length);

      FunnelHeatMapRow cumulativeFilteredRow = new FunnelHeatMapRow(row.getBaselineTime(), cumulativeBaselineCopy,
          row.getCurrentTime(), cumulativeCurrentCopy);
      filteredCumulativeTable.add(cumulativeFilteredRow);
    }
    return new FunnelHeatMapView(spec, filteredTable, filteredCumulativeTable, currentEnd, baselineEnd);
  }

  // TODO : {dpatel : move this to config cache later, would have started with it but found that out late}
  private void loadConfigs() {
    // looping throuh all the dirs, finding one funnels file and loading it up
    ObjectMapper ymlReader = new ObjectMapper(new YAMLFactory());
    for (File f : funnelsRoot.listFiles()) {
      File funnelsFile = new File(f, FUNNELS_CONFIG_FILE_NAME);
      if (!funnelsFile.exists()) {
        LOG.warn("did not find funnels config file {} , in folder {}", FUNNELS_CONFIG_FILE_NAME, f.getName());
        continue;
      }

      try {
        CustomFunnelSpec spec = ymlReader.readValue(funnelsFile, CustomFunnelSpec.class);
        this.funnelSpecsMap.put(spec.getCollection(), spec);
      } catch (Exception e) {
        LOG.error("error loading the configFile", e);
      }
    }
  }
}
