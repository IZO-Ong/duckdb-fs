#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

struct ServeFeatureBindData : public FunctionData {
	string feature_names; // comma-separated
	string spine_table;
	string entity_override; // comma-separated spine entity columns; empty = use feature's entity columns
	string as_of_column;    // empty = use feature's timestamp column name (assumes spine has same name)
	vector<string> result_names;
	vector<LogicalType> result_types;
	string generated_sql;
	string default_catalog; // caller's default catalog for sub-connections
	string default_schema;  // caller's default schema for sub-connections

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ServeFeatureBindData>();
		result->feature_names = feature_names;
		result->spine_table = spine_table;
		result->entity_override = entity_override;
		result->as_of_column = as_of_column;
		result->result_names = result_names;
		result->result_types = result_types;
		result->generated_sql = generated_sql;
		result->default_catalog = default_catalog;
		result->default_schema = default_schema;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<ServeFeatureBindData>();
		return feature_names == o.feature_names && spine_table == o.spine_table &&
		       entity_override == o.entity_override && as_of_column == o.as_of_column;
	}
};

struct ServeFeatureState : public GlobalTableFunctionState {
	bool done = false;
	unique_ptr<QueryResult> query_result;
};

static string QuoteId(const string &name) {
	return SQLIdentifier::ToString(name);
}

static optional_ptr<FeatureCatalogEntry> LookupFeature(ClientContext &context, const string &feature_name);

//! Build the entity ASOF-join condition and the EXCLUDE list for a single feature.
//! - entity_overrides: spine-side column names (one per feature entity column). Empty => the spine uses
//!   the same column names as the feature.
//! - With zero entity columns (global feature) the join carries only the temporal condition and the
//!   EXCLUDE list contains just feature_timestamp.
static void BuildEntityJoin(const FeatureCatalogEntry &feat, const vector<string> &entity_overrides,
                            const string &feat_alias, string &entity_cond, string &exclude_list) {
	entity_cond.clear();
	exclude_list.clear();
	for (idx_t i = 0; i < feat.entity_columns.size(); i++) {
		auto feat_entity = QuoteId(feat.entity_columns[i]);
		auto spine_entity = entity_overrides.empty() ? feat_entity : QuoteId(entity_overrides[i]);
		entity_cond += StringUtil::Format("spine.%s = %s.%s AND ", spine_entity, feat_alias, feat_entity);
		exclude_list += feat_entity + ", ";
	}
	exclude_list += "feature_timestamp";
}

//! Split a comma-separated override string into trimmed column names (empty input => empty vector).
static vector<string> ParseEntityOverrides(const string &entity_override) {
	vector<string> result;
	if (entity_override.empty()) {
		return result;
	}
	result = StringUtil::Split(entity_override, ',');
	for (auto &c : result) {
		StringUtil::Trim(c);
	}
	return result;
}

static void ValidateEntityOverride(const FeatureCatalogEntry &feat, const vector<string> &entity_overrides) {
	if (!entity_overrides.empty() && entity_overrides.size() != feat.entity_columns.size()) {
		throw BinderException(
		    "ENTITY override for feature \"%s\" has %llu column(s) but the feature has %llu entity column(s)",
		    feat.name, (idx_t)entity_overrides.size(), (idx_t)feat.entity_columns.size());
	}
}

static string BuildServeSQL(ClientContext &context, const vector<string> &feature_list, const string &spine_table,
                            const string &entity_override, const string &as_of_override) {
	// For each feature, build an ASOF JOIN against the spine
	// Only uses the current version from the feature metadata
	// entity_override: comma-separated spine entity columns (one per feature entity column); empty => the
	//   spine uses the same column names as the feature. For a global feature (no entity) the ASOF join
	//   degenerates to the temporal condition alone.
	// as_of_override: if non-empty, use this as the spine's timestamp column
	auto entity_overrides = ParseEntityOverrides(entity_override);

	// Validate that spine table exists
	auto schemas = Catalog::GetAllSchemas(context);
	bool spine_found = false;
	for (auto &schema : schemas) {
		auto entry =
		    schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::TABLE_ENTRY, spine_table);
		if (entry) {
			spine_found = true;
			break;
		}
	}
	if (!spine_found) {
		throw CatalogException("Spine table \"%s\" does not exist", spine_table);
	}

	// For multiple features without an entity override, validate they all share the same entity columns
	if (feature_list.size() > 1 && entity_override.empty()) {
		vector<string> first_entities;
		bool first = true;
		for (auto &fname : feature_list) {
			auto entry = LookupFeature(context, fname);
			if (!entry) {
				throw CatalogException("Feature \"%s\" does not exist", fname);
			}
			if (first) {
				first_entities = entry->entity_columns;
				first = false;
			} else if (entry->entity_columns != first_entities) {
				throw BinderException("Features have different entity columns. "
				                      "Use ENTITY to specify the spine's entity column(s) explicitly.");
			}
		}
	}

	if (feature_list.size() == 1) {
		// Single feature: simple ASOF JOIN
		auto feature_entry = LookupFeature(context, feature_list[0]);
		if (!feature_entry) {
			throw CatalogException("Feature \"%s\" does not exist", feature_list[0]);
		}
		auto &feat = *feature_entry;
		ValidateEntityOverride(feat, entity_overrides);
		auto feat_table = QuoteId(feat.name);
		auto spine_ts = as_of_override.empty() ? QuoteId(feat.timestamp_column) : QuoteId(as_of_override);
		auto spine = QuoteId(spine_table);

		string entity_cond, exclude_list;
		BuildEntityJoin(feat, entity_overrides, "f", entity_cond, exclude_list);

		return StringUtil::Format("SELECT spine.*, f.feature_timestamp, "
		                          "f.* EXCLUDE (%s) "
		                          "FROM %s AS spine "
		                          "ASOF LEFT JOIN %s AS f "
		                          "ON %sspine.%s >= f.feature_timestamp",
		                          exclude_list, // EXCLUDE
		                          spine,        // FROM
		                          feat_table,   // feature view (points to latest version)
		                          entity_cond,  // entity join (empty for global feature)
		                          spine_ts      // temporal condition
		);
	}

	// Multiple features: chain of ASOF JOINs
	string sql = "SELECT spine.*";
	string joins;

	for (idx_t i = 0; i < feature_list.size(); i++) {
		auto feature_entry = LookupFeature(context, feature_list[i]);
		if (!feature_entry) {
			throw CatalogException("Feature \"%s\" does not exist", feature_list[i]);
		}
		auto &feat = *feature_entry;
		ValidateEntityOverride(feat, entity_overrides);
		auto feat_table = QuoteId(feat.name);
		auto spine_ts = as_of_override.empty() ? QuoteId(feat.timestamp_column) : QuoteId(as_of_override);
		auto alias = "f" + duckdb::to_string(i);

		string entity_cond, exclude_list;
		BuildEntityJoin(feat, entity_overrides, alias, entity_cond, exclude_list);

		sql += StringUtil::Format(", %s.feature_timestamp AS %s_timestamp, %s.* EXCLUDE (%s)", alias, feat.name, alias,
		                          exclude_list);

		joins += StringUtil::Format(" ASOF LEFT JOIN %s AS %s "
		                            "ON %sspine.%s >= %s.feature_timestamp",
		                            feat_table, alias, entity_cond, spine_ts, alias);
	}

	sql += " FROM " + QuoteId(spine_table) + " AS spine" + joins;
	return sql;
}

static void SetConnectionCatalog(Connection &con, const string &catalog, const string &schema) {
	if (!catalog.empty()) {
		auto use_result = con.Query("USE " + QuoteId(catalog));
		if (use_result->HasError()) {
			throw InternalException("Failed to set catalog for SERVE FEATURE: %s", use_result->GetError());
		}
	}
	if (!schema.empty() && schema != DEFAULT_SCHEMA) {
		auto schema_result = con.Query("SET schema = '" + schema + "'");
		if (schema_result->HasError()) {
			throw InternalException("Failed to set schema for SERVE FEATURE: %s", schema_result->GetError());
		}
	}
}

static optional_ptr<FeatureCatalogEntry> LookupFeature(ClientContext &context, const string &feature_name) {
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::FEATURE_ENTRY,
		                                   feature_name);
		if (entry) {
			return &entry->Cast<FeatureCatalogEntry>();
		}
	}
	return nullptr;
}

static unique_ptr<FunctionData> ServeFeatureBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ServeFeatureBindData>();
	result->feature_names = input.inputs[0].GetValue<string>();
	result->spine_table = input.inputs[1].GetValue<string>();
	result->entity_override = input.inputs[2].GetValue<string>();
	result->as_of_column = input.inputs[3].GetValue<string>();

	// Parse feature names (comma-separated)
	vector<string> feature_list = StringUtil::Split(result->feature_names, ',');
	for (auto &f : feature_list) {
		StringUtil::Trim(f);
	}

	// Capture the caller's default catalog/schema so sub-connections resolve tables correctly
	auto &search_path = ClientData::Get(context).catalog_search_path;
	auto &default_entry = search_path->GetDefault();
	result->default_catalog = default_entry.catalog;
	result->default_schema = default_entry.schema;

	// Build the SQL — empty entity/as_of means "use same column name as feature metadata"
	result->generated_sql =
	    BuildServeSQL(context, feature_list, result->spine_table, result->entity_override, result->as_of_column);

	// Use a sub-connection to determine the result schema
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);
	SetConnectionCatalog(con, result->default_catalog, result->default_schema);
	auto prep = con.Prepare(result->generated_sql);
	if (prep->HasError()) {
		throw BinderException("Failed to prepare SERVE FEATURE query: %s\nGenerated SQL: %s", prep->GetError(),
		                      result->generated_sql);
	}

	names = prep->GetNames();
	return_types = prep->GetTypes();

	result->result_names = names;
	result->result_types = return_types;

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ServeFeatureInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ServeFeatureState>();
}

static void ServeFeatureFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ServeFeatureState>();
	auto &bind_data = data_p.bind_data->Cast<ServeFeatureBindData>();

	if (state.done) {
		return;
	}

	// Execute the query on first call
	if (!state.query_result) {
		auto &db = DatabaseInstance::GetDatabase(context);
		Connection con(db);
		SetConnectionCatalog(con, bind_data.default_catalog, bind_data.default_schema);
		state.query_result = con.Query(bind_data.generated_sql);
		if (state.query_result->HasError()) {
			throw InternalException("SERVE FEATURE query failed: %s", state.query_result->GetError());
		}
	}

	// Fetch next chunk
	auto chunk = state.query_result->Fetch();
	if (!chunk || chunk->size() == 0) {
		state.done = true;
		return;
	}

	output.Move(*chunk);
}

void ServeFeatureFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction(
	    "serve_feature", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    ServeFeatureFunction, ServeFeatureBind, ServeFeatureInit));
}

} // namespace duckdb
