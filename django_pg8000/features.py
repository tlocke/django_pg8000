from django.db import DatabaseError, InterfaceError
from django.db.backends.base.features import BaseDatabaseFeatures
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):
    minimum_database_version = (12,)
    allows_group_by_selected_pks = True
    can_return_columns_from_insert = True
    can_return_rows_from_bulk_insert = True
    has_real_datatype = True
    has_native_uuid_field = True
    has_native_duration_field = True
    has_native_json_field = True
    can_defer_constraint_checks = True
    has_select_for_update = True
    has_select_for_update_nowait = True
    has_select_for_update_of = True
    has_select_for_update_skip_locked = True
    has_select_for_no_key_update = True
    can_release_savepoints = True
    supports_comments = True
    supports_tablespaces = True
    supports_transactions = True
    can_introspect_materialized_views = True
    can_distinct_on_fields = True
    can_rollback_ddl = True
    schema_editor_uses_clientside_param_binding = False
    supports_combined_alters = True
    supports_non_deterministic_collations = False
    nulls_order_largest = True
    closed_cursor_error_class = InterfaceError
    greatest_least_ignores_nulls = True
    can_clone_databases = True
    supports_paramstyle_pyformat = False
    supports_temporal_subtraction = True
    supports_slicing_ordering_in_compound = True
    create_test_procedure_without_params_sql = """
        CREATE FUNCTION test_procedure () RETURNS void AS $$
        DECLARE
            V_I INTEGER;
        BEGIN
            V_I := 1;
        END;
    $$ LANGUAGE plpgsql;"""
    create_test_procedure_with_int_param_sql = """
        CREATE FUNCTION test_procedure (P_I INTEGER) RETURNS void AS $$
        DECLARE
            V_I INTEGER;
        BEGIN
            V_I := P_I;
        END;
    $$ LANGUAGE plpgsql;"""
    create_test_table_with_composite_primary_key = """
        CREATE TABLE test_table_composite_pk (
            column_1 INTEGER NOT NULL,
            column_2 INTEGER NOT NULL,
            PRIMARY KEY(column_1, column_2)
        )
    """
    requires_casted_case_in_updates = True
    supports_over_clause = True
    only_supports_unbounded_with_preceding_and_following = True
    supports_aggregate_filter_clause = True
    supported_explain_formats = {"JSON", "TEXT", "XML", "YAML"}
    supports_deferrable_unique_constraints = True
    supports_json_field = False
    has_json_object_function = False
    has_json_operators = True
    json_key_contains_list_matching_requires_list = True
    supports_update_conflicts = False
    supports_update_conflicts_with_target = True
    supports_covering_indexes = True
    can_rename_index = True
    test_collations = {
        "deterministic": "C",
        "non_default": "sv-x-icu",
        "swedish_ci": "sv-x-icu",
        "virtual": "sv-x-icu",
    }
    test_now_utc_template = "STATEMENT_TIMESTAMP() AT TIME ZONE 'UTC'"
    insert_test_table_with_defaults = "INSERT INTO {} DEFAULT VALUES"

    @cached_property
    def prohibits_null_characters_in_text_exception(self):
        return (
            DatabaseError,
            "{'S': 'ERROR', 'V': 'ERROR', 'C': '08P01', 'M': 'invalid message format'",
        )

    @cached_property
    def introspected_field_types(self):
        return {
            **super().introspected_field_types,
            "PositiveBigIntegerField": "BigIntegerField",
            "PositiveIntegerField": "IntegerField",
            "PositiveSmallIntegerField": "SmallIntegerField",
        }

    @cached_property
    def is_postgresql_13(self):
        return self.connection.pg_version >= 130000

    @cached_property
    def is_postgresql_14(self):
        return self.connection.pg_version >= 140000

    @cached_property
    def is_postgresql_15(self):
        return self.connection.pg_version >= 150000

    @cached_property
    def has_bit_xor(self):
        return self.is_postgresql_14

    @cached_property
    def supports_coverting_spgist_indexes(self):
        return self.is_postgresql_14

    supports_unlimited_charfield = True

    @cached_property
    def supports_nulls_distinct_unique_constraints(self):
        return self.is_postgresql_15

    @cached_property
    def supports_covering_spgist_indexes(self):
        return self.is_postgresql_14
