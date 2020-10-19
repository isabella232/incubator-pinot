package org.apache.pinot.tools.admin.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.QuickstartTableRequest;


public class JSONQuickstart {
  private static final String TAB = "\t\t";
  private static final String NEW_LINE = "\n";

  public enum Color {
    RESET("\u001B[0m"), GREEN("\u001B[32m"), YELLOW("\u001B[33m"), CYAN("\u001B[36m");

    public String _code;

    Color(String code) {
      _code = code;
    }
  }

  public static void printStatus(JSONQuickstart.Color color, String message) {
    System.out.println(color._code + message + JSONQuickstart.Color.RESET._code);
  }

  public static String prettyPrintResponse(JsonNode response) {
    StringBuilder responseBuilder = new StringBuilder();

    // Sql Results
    if (response.has("resultTable")) {
      JsonNode columns = response.get("resultTable").get("dataSchema").get("columnNames");
      int numColumns = columns.size();
      for (int i = 0; i < numColumns; i++) {
        responseBuilder.append(columns.get(i).asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JsonNode rows = response.get("resultTable").get("rows");
      for (int i = 0; i < rows.size(); i++) {
        JsonNode row = rows.get(i);
        for (int j = 0; j < numColumns; j++) {
          responseBuilder.append(row.get(j).asText()).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
      return responseBuilder.toString();
    }

    // Selection query
    if (response.has("selectionResults")) {
      JsonNode columns = response.get("selectionResults").get("columns");
      int numColumns = columns.size();
      for (int i = 0; i < numColumns; i++) {
        responseBuilder.append(columns.get(i).asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JsonNode rows = response.get("selectionResults").get("results");
      int numRows = rows.size();
      for (int i = 0; i < numRows; i++) {
        JsonNode row = rows.get(i);
        for (int j = 0; j < numColumns; j++) {
          responseBuilder.append(row.get(j).asText()).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
      return responseBuilder.toString();
    }

    // Aggregation only query
    if (!response.get("aggregationResults").get(0).has("groupByResult")) {
      JsonNode aggregationResults = response.get("aggregationResults");
      int numAggregations = aggregationResults.size();
      for (int i = 0; i < numAggregations; i++) {
        responseBuilder.append(aggregationResults.get(i).get("function").asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      for (int i = 0; i < numAggregations; i++) {
        responseBuilder.append(aggregationResults.get(i).get("value").asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      return responseBuilder.toString();
    }

    // Aggregation group-by query
    JsonNode groupByResults = response.get("aggregationResults");
    int numGroupBys = groupByResults.size();
    for (int i = 0; i < numGroupBys; i++) {
      JsonNode groupByResult = groupByResults.get(i);
      responseBuilder.append(groupByResult.get("function").asText()).append(TAB);
      JsonNode columns = groupByResult.get("groupByColumns");
      int numColumns = columns.size();
      for (int j = 0; j < numColumns; j++) {
        responseBuilder.append(columns.get(j).asText()).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JsonNode rows = groupByResult.get("groupByResult");
      int numRows = rows.size();
      for (int j = 0; j < numRows; j++) {
        JsonNode row = rows.get(j);
        responseBuilder.append(row.get("value").asText()).append(TAB);
        JsonNode columnValues = row.get("group");
        for (int k = 0; k < numColumns; k++) {
          responseBuilder.append(columnValues.get(k).asText()).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
    }
    return responseBuilder.toString();
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));
    File configDir = new File(quickstartTmpDir, "configs");
    File dataDir = new File(quickstartTmpDir, "data");
    Preconditions.checkState(configDir.mkdirs());
    Preconditions.checkState(dataDir.mkdirs());

    File schemaFile = new File(configDir, "super_schema.json");
    File dataFile = new File(configDir, "super_data.csv");
    File tableConfigFile = new File(configDir, "super_offline_table_config.json");
    File ingestionJobSpecFile = new File(configDir, "ingestionJobSpec.yaml");

    ClassLoader classLoader = JSONQuickstart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/batch/super/super_schema.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource("examples/batch/super/rawdata/super_data.csv");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, dataFile);
    resource = classLoader.getResource("examples/batch/super/ingestionJobSpec.yaml");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
    resource = classLoader.getResource("examples/batch/super/super_offline_table_config.json");
    com.google.common.base.Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    QuickstartTableRequest request =
        new QuickstartTableRequest("super", schemaFile, tableConfigFile, ingestionJobSpecFile, FileFormat.CSV);
    final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, dataDir);

    printStatus(JSONQuickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker and server *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(JSONQuickstart.Color.GREEN, "***** Shutting down offline quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    printStatus(JSONQuickstart.Color.CYAN, "***** Adding super table *****");
    runner.addTable();
    printStatus(JSONQuickstart.Color.CYAN,
        "***** Launch data ingestion job to build index segment for super and push to controller *****");
    runner.launchDataIngestionJob();
    printStatus(JSONQuickstart.Color.CYAN,
        "***** Waiting for 5 seconds for the server to fetch the assigned segment *****");
    Thread.sleep(5000);

    printStatus(JSONQuickstart.Color.YELLOW, "***** Offline quickstart setup complete *****");

  }

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
//    generateFile(new File(
//        "pinot-tools/src/main/resources/examples/batch/super/rawdata/super_data.csv"));

    new JSONQuickstart().execute();
  }

  static void generateFile(File file)
      throws IOException {
    if(false) {
      return;
    }
    ObjectMapper mapper = new ObjectMapper();
    Random random = new Random();
    file.delete();
    BufferedWriter bw = new BufferedWriter(new FileWriter(file, false));
//    CSVPrinter csvPrinter = new CSVPrinter(bw, CSVFormat.newFormat(',').withRecordSeparator('\n'));
//    csvPrinter.printRecord("person");
    bw.write("person");
    bw.newLine();
    for (int i = 0; i < 100; i++) {
      ObjectNode person = mapper.createObjectNode();
      person.put("name", "adam-" + i);
      person.put("age", random.nextInt(100));
      ArrayNode arrayNode = mapper.createArrayNode();
      for (int j = 0; j < 3; j++) {
        ObjectNode address = mapper.createObjectNode();
        address.put("street", "street-" + i);
        address.put("country", "us");
        arrayNode.add(address);
      }
      person.set("addresses", arrayNode);
      String personJSON = mapper.writer().writeValueAsString(person);
//      csvPrinter.printRecord(personJSON.replaceAll("\"", "\"\""));
      bw.write("\"");
      bw.write(personJSON.replaceAll("\"", "\"\""));
      bw.write("\"");
      bw.newLine();

    }
//    csvPrinter.close();
//    System.out.println("csvPrinter = " + csvPrinter);
    bw.close();
  }
}
