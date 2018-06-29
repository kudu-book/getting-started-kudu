/// Kudu C++ client API example
///
/// See README.md file for details on compiling and running this application.
/// API documentation: https://kudu.apache.org/cpp-client-api

#include <ctime>
#include <iostream>
#include <sstream>

#include "kudu/client/callbacks.h"
#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/stubs.h"
#include "kudu/client/value.h"
#include "kudu/common/partial_row.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduPredicate;
using kudu::client::KuduRowResult;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduStatusFunctionCallback;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;
using kudu::MonoDelta;
using kudu::Status;

using std::ostringstream;
using std::string;
using std::vector;

int main(int argc, char* argv[]) {
    KUDU_LOG(INFO) << "Running with Kudu client version: " <<
        kudu::client::GetShortVersionString();

    if (argc != 2) {
        KUDU_LOG(FATAL) << "Usage: " << argv[0] << " <master-host>";
    }

    const string masterHost = argv[1];
    const string kTableName = "k_sample_table";

    // Log levels from 0 to 6. Higher the number, the higher the amount of
    // logging.
    kudu::client::SetVerboseLogLevel(0);

    // Create and connect a client
    shared_ptr<KuduClient> client;
    KuduClientBuilder()
        .add_master_server_addr(masterHost)
        .default_admin_operation_timeout(MonoDelta::FromSeconds(10))
        .Build(&client);

    // Create a schema
    KuduSchema schema;
    KuduSchemaBuilder sb;
    sb.AddColumn("id")       ->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    sb.AddColumn("lastname") ->Type(KuduColumnSchema::STRING)->NotNull();
    sb.AddColumn("firstname")->Type(KuduColumnSchema::STRING)->NotNull();
    sb.AddColumn("city")     ->Type(KuduColumnSchema::STRING)->NotNull()->Default(KuduValue::CopyString("Toronto"));
    KUDU_CHECK_OK(sb.Build(&schema));

    KUDU_LOG(INFO) << "Created a schema";

    // Check if a table exists
    shared_ptr<KuduTable> table;
    Status s = client->OpenTable(kTableName, &table);
    if (s.ok()) {
        // Table exists since we were able to open it
        KUDU_LOG(INFO) << "Table " << kTableName << " exists, deleting it!";
        client->DeleteTable(kTableName);
        KUDU_LOG(INFO) << "Deleted.";
    } else if (s.IsNotFound()) {
        // Table does not exist just continue
        KUDU_LOG(INFO) << "Table " << kTableName << " does not exist";
    }

    // Create the table
    KUDU_LOG(INFO) << "Creating table " << kTableName;
    // 1. Split/partitioning keys
    // The ranges here are 0 -> 999, 1000 -> 1999, 2000 -> 2999, 3000+
    KUDU_LOG(INFO) << "-- Defining splits";
    vector<const KuduPartialRow *> splits;
    // Generate a new KuduPartialRow from the schema
    KuduPartialRow *row = schema.NewRow();
    row->SetInt32("id", 1000);
    splits.push_back(row);
    row = schema.NewRow();
    row->SetInt32("id", 2000);
    splits.push_back(row);
    row = schema.NewRow();
    row->SetInt32("id", 3000);
    splits.push_back(row);

    // 2. Key column
    KUDU_LOG(INFO) << "-- Defining range partition columns";
    vector<string> columnNames;
    columnNames.push_back("id");

    // 3. Create the table
    KUDU_LOG(INFO) << "-- Creating the table itself";
    KuduTableCreator *tableCreator = client->NewTableCreator();
    s = tableCreator->table_name(kTableName)
        .schema(&schema)
        .set_range_partition_columns(columnNames)
        .split_rows(splits)
        .num_replicas(1)
        .Create();
    KUDU_CHECK_OK(s);
    delete tableCreator;
}
