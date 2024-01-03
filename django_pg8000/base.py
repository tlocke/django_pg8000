import asyncio
import codecs
import csv
import threading
import warnings
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime as Datetime
from io import StringIO
from itertools import islice

from dateutil.parser import parse

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    DataError as WrappedDataError,
    DatabaseError as WrappedDatabaseError,
    IntegrityError as WrappedIntegrityError,
    ProgrammingError as WrappedProgrammingError,
    connections,
)
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import CursorDebugWrapper
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property

import pg8000.dbapi
from pg8000.converters import (
    DATERANGE,
    INET_ARRAY,
    INT4RANGE,
    INT4RANGE_ARRAY,
    INT8RANGE,
    JSONB_ARRAY,
    JSON_ARRAY,
    NUMRANGE,
    TIMESTAMPTZ_ARRAY,
    int4range_array_in,
    int_in,
    string_array_in,
    string_in,
    timestamptz_array_in,
)
from pg8000.core import Context, IN_FAILED_TRANSACTION, IN_TRANSACTION
from pg8000.native import (
    Connection,
    DatabaseError,
    Error,
    INET,
    InterfaceError,
    JSON,
    JSONB,
    TIMESTAMPTZ,
    identifier,
)


from django_pg8000.client import DatabaseClient
from django_pg8000.creation import DatabaseCreation
from django_pg8000.features import DatabaseFeatures
from django_pg8000.introspection import DatabaseIntrospection
from django_pg8000.operations import DatabaseOperations
from django_pg8000.schema import DatabaseSchemaEditor


def _get_varchar_column(data):
    if data["max_length"] is None:
        return "varchar"
    return "varchar(%(max_length)s)" % data


class Range:
    def __init__(
        self,
        lower=None,
        upper=None,
        bounds="[)",
        is_empty=False,
    ):
        self.lower = lower
        self.upper = upper
        self.bounds = bounds
        self.is_empty = is_empty

    def __eq__(self, other):
        if isinstance(other, Range):
            if self.is_empty or other.is_empty:
                return self.is_empty == other.is_empty
            else:
                return (
                    self.lower == other.lower
                    and self.upper == other.upper
                    and self.bounds == other.bounds
                )
        return False

    def __str__(self):
        if self.is_empty:
            return "empty"
        else:
            le, ue = ["" if v is None else v for v in (self.lower, self.upper)]
            return f"{self.bounds[0]}{le},{ue}{self.bounds[1]}"

    def __repr__(self):
        return f"<Range {self}>"


def make_range_in(elem_func):
    def range_in(data):
        if data == "empty":
            return Range(is_empty=True)
        else:
            le, ue = [None if v == "" else elem_func(v) for v in data[1:-1].split(",")]
            return Range(le, ue, bounds=f"{data[0]}{data[-1]}")

    return range_in


int4range_in = make_range_in(int_in)


def timestamptz_in_use_tz(value):
    try:
        patt = "%Y-%m-%d %H:%M:%S.%f%z" if "." in value else "%Y-%m-%d %H:%M:%S%z"
        return Datetime.strptime(f"{value}00", patt)
    except ValueError:
        return parse(value)


def timestamptz_in_no_tz(value):
    # We set the time zone to UTC when the connection is created
    return timestamptz_in_use_tz(value).replace(tzinfo=None)


class Info:
    def __init__(self, con):
        self.con = con

    @property
    def _parameter_statuses(self):
        return self.con.parameter_statuses

    def get_parameter_status(self, name):
        return self._parameter_statuses.get(name)

    @property
    def encoding(self):
        pg_encoding = self.get_parameter_status("client_encoding")
        codec_info = codecs.lookup(pg_encoding)
        return codec_info.name


class DatabaseConnection:
    def __init__(self, conn_params):
        if "client_encoding" in conn_params:
            del conn_params["client_encoding"]

        if "cursor_factory" in conn_params:
            self.cursor_factory = conn_params["cursor_factory"]
            del conn_params["cursor_factory"]
        else:
            self.cursor_factory = DatabaseCursor

        if "server_side_binding" in conn_params:
            self.server_side_binding = conn_params["server_side_binding"]
            del conn_params["server_side_binding"]
        else:
            self.server_side_binding = False

        c = Connection(**conn_params)

        c.register_in_adapter(JSON, string_in)
        c.register_in_adapter(JSON_ARRAY, string_array_in)
        c.register_in_adapter(JSONB, string_in)
        c.register_in_adapter(JSONB_ARRAY, string_array_in)
        c.register_in_adapter(INET, string_in)
        c.register_in_adapter(INET_ARRAY, string_array_in)
        c.register_in_adapter(INT4RANGE, string_array_in)

        c.register_in_adapter(INT4RANGE_ARRAY, int4range_array_in)

        c.register_in_adapter(INT8RANGE, string_array_in)
        c.register_in_adapter(DATERANGE, string_array_in)
        c.register_in_adapter(NUMRANGE, string_array_in)

        self.info = Info(c)

        if self.info.encoding != "utf-8":
            c.run("SET CLIENT_ENCODING TO 'UTF8'")

        self._con = c
        self.autocommit = False
        self.isolation_level = None
        self._isolation_level_sql = None

    def commit(self):
        try:
            self._con.run("COMMIT")
        except InterfaceError as e:
            # Django expects commits on failed transactions to not raise an error
            if str(e) != "in failed transaction block":
                raise e
        except DatabaseError as e:
            error_arg = e.args[0]
            error_code = error_arg["C"]
            error_message = error_arg["M"]
            if error_code.startswith("23"):
                raise WrappedIntegrityError(error_message)
            else:
                raise

    def close(self):
        self._con.close()

    def rollback(self):
        self._con.run("ROLLBACK")

    @property
    def _in_transaction(self):
        return self._con._transaction_status in (IN_TRANSACTION, IN_FAILED_TRANSACTION)

    def _run(self, sql, params=(), stream=None):
        return self._con.run(sql, params=params, stream=stream)

    def _execute_simple(self, sql):
        return self._con.execute_simple(sql)

    def _execute_unnamed(self, sql, stream=None):
        return self._con.execute_unnamed(sql, stream=stream)


class DatabaseCopy:
    def __init__(self, cursor, sql):
        self.cursor = cursor
        self.sql = sql
        stream_out = StringIO()
        self.cursor.execute(sql, stream=stream_out)
        stream_out.seek(0)
        self.reader = csv.reader(stream_out)

    def __iter__(self):
        return self.reader

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


DescriptionColumn = namedtuple(
    "DescriptionColumn",
    "name type_code display_size internal_size precision scale null_ok",
)


class DatabaseCursor:
    def __init__(self, con):
        self.con = con
        self._context = None
        self._row_iter = None
        self.arraysize = 1

    def execute(self, operation, args=(), stream=None):
        if self.con is None:
            raise InterfaceError("Cursor is closed.")
        if self.con.server_side_binding:
            sql = operation
        else:
            sql = DatabaseOperations._compose_sql(operation, args)

        """
        print("operation", operation)
        print("args", args)
        print("sql", sql)
        """
        try:
            if not self.con._in_transaction and not self.con.autocommit:
                tsql = "START TRANSACTION"
                if self.con._isolation_level_sql is not None:
                    tsql += (
                        f" ISOLATION LEVEL {identifier(self.con._isolation_level_sql)}"
                    )
                self.con._run(tsql)

            if stream is None:
                self._context = self.con._execute_simple(sql)
            else:
                self._context = self.con._execute_unnamed(sql, stream=stream)

            if self._context.rows is None:
                self._row_iter = None
            else:
                self._row_iter = iter([tuple(r) for r in self._context.rows])

        except DatabaseError as e:
            error_arg = e.args[0]
            error_code = error_arg["C"]
            error_message = error_arg["M"]
            if error_code.startswith("23"):
                raise WrappedIntegrityError(error_message)
            elif error_code.startswith("22"):
                raise WrappedDataError(error_message)
            elif error_code in ("42710", "42704", "42P01"):
                raise WrappedProgrammingError(error_message)
            else:
                raise

        # print("description", self._context.columns)

    def executemany(self, operation, param_sets):
        rowcounts = []
        for parameters in param_sets:
            self.execute(operation, parameters)
            rowcounts.append(self._context.row_count)

        if len(rowcounts) == 0:
            self._context = Context(None)
        elif -1 in rowcounts:
            self._context.row_count = -1
        else:
            self._context.row_count = sum(rowcounts)

    def fetchone(self):
        try:
            return next(self)
        except StopIteration:
            return None
        except TypeError:
            raise InterfaceError("attempting to use unexecuted cursor")

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._row_iter)
        except AttributeError:
            if self._context is None:
                raise InterfaceError("A query hasn't been issued.")
            else:
                raise
        except StopIteration as e:
            if self._context is None:
                raise InterfaceError("A query hasn't been issued.")
            elif len(self._context.columns) == 0:
                raise InterfaceError("no result set")
            else:
                raise e

    def fetchmany(self, num=None):
        try:
            return list(islice(self, self.arraysize if num is None else num))
        except TypeError:
            raise InterfaceError("attempting to use unexecuted cursor")

    def fetchall(self):
        try:
            return list(self)
        except TypeError:
            raise InterfaceError("attempting to use unexecuted cursor")

    def close(self):
        self.con = None

    @property
    def closed(self):
        return self.con is None

    @property
    def rowcount(self):
        context = self._context
        if context is None:
            return -1

        return context.row_count

    @property
    def description(self):
        context = self._context
        if context is None:
            return None

        row_desc = context.columns
        if row_desc is None:
            return None
        if len(row_desc) == 0:
            return None
        columns = [
            DescriptionColumn(
                col["name"], col["type_oid"], None, None, None, None, None
            )
            for col in row_desc
        ]
        return columns

    def callproc(self, name, params=()):
        args = "".join(DatabaseOperations._literal(p) for p in params)
        self.execute(f"SELECT {identifier(name)}({args})")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def copy(self, sql):
        return DatabaseCopy(self, sql)


class ServerSideCursor:
    def __init__(self, cursor, name):
        self.cursor = cursor
        self.name = name

    def execute(self, operation, args=()):
        self.cursor.con.autocommit = False
        operation = f"DECLARE {self.name} NO SCROLL CURSOR FOR {operation}"
        self.cursor.execute(operation, args)

    def executemany(self, operation, param_sets):
        self.cursor.executemany(operation, param_sets)

    def fetchone(self):
        self.cursor.execute(f"FETCH FORWARD 1 FROM {self.name}")
        return self.cursor.fetchone()

    def fetchmany(self, num=None):
        if num is None:
            return self.cursor.fetchall()
        else:
            self.cursor.execute(f"FETCH FORWARD {int(num)} FROM {self.name}")
            return self.cursor.fetchall()

    def fetchall(self):
        self.cursor.execute(f"FETCH FORWARD ALL FROM {self.name}")
        return self.cursor.fetchall()

    def close(self):
        self.cursor.execute(f"CLOSE {self.name}")
        self.cursor.close()

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = "postgresql_pg8000"
    display_name = "PostgreSQL"
    # This dictionary maps Field objects to their associated PostgreSQL column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__ before being output.
    # If a column type is set to None, it won't be included in the output.
    data_types = {
        "AutoField": "integer",
        "BigAutoField": "bigint",
        "BinaryField": "bytea",
        "BooleanField": "boolean",
        "CharField": _get_varchar_column,
        "DateField": "date",
        "DateTimeField": "timestamp with time zone",
        "DecimalField": "numeric(%(max_digits)s, %(decimal_places)s)",
        "DurationField": "interval",
        "FileField": "varchar(%(max_length)s)",
        "FilePathField": "varchar(%(max_length)s)",
        "FloatField": "double precision",
        "IntegerField": "integer",
        "BigIntegerField": "bigint",
        "IPAddressField": "inet",
        "GenericIPAddressField": "inet",
        "JSONField": "jsonb",
        "OneToOneField": "integer",
        "PositiveBigIntegerField": "bigint",
        "PositiveIntegerField": "integer",
        "PositiveSmallIntegerField": "smallint",
        "SlugField": "varchar(%(max_length)s)",
        "SmallAutoField": "smallint",
        "SmallIntegerField": "smallint",
        "TextField": "text",
        "TimeField": "time",
        "UUIDField": "uuid",
    }
    data_type_check_constraints = {
        "PositiveBigIntegerField": '"%(column)s" >= 0',
        "PositiveIntegerField": '"%(column)s" >= 0',
        "PositiveSmallIntegerField": '"%(column)s" >= 0',
    }
    data_types_suffix = {
        "AutoField": "GENERATED BY DEFAULT AS IDENTITY",
        "BigAutoField": "GENERATED BY DEFAULT AS IDENTITY",
        "SmallAutoField": "GENERATED BY DEFAULT AS IDENTITY",
    }
    operators = {
        "exact": "= %s",
        "iexact": "= UPPER(%s)",
        "contains": "LIKE %s",
        "icontains": "LIKE UPPER(%s)",
        "regex": "~ %s",
        "iregex": "~* %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s",
        "endswith": "LIKE %s",
        "istartswith": "LIKE UPPER(%s)",
        "iendswith": "LIKE UPPER(%s)",
    }

    # The patterns below are used to generate SQL pattern lookup clauses when
    # the right-hand side of the lookup isn't a raw string (it might be an expression
    # or the result of a bilateral transformation).
    # In those cases, special characters for LIKE operators (e.g. \, *, _) should be
    # escaped on database side.
    #
    # Note: we use str.format() here for readability as '%' is used as a wildcard for
    # the LIKE operator.
    pattern_esc = (
        r"REPLACE(REPLACE(REPLACE({}, E'\\', E'\\\\'), E'%%', E'\\%%'), E'_', E'\\_')"
    )

    pattern_ops = {
        "contains": "LIKE '%%' || {} || '%%'",
        "icontains": "LIKE '%%' || UPPER({}) || '%%'",
        "startswith": "LIKE {} || '%%'",
        "istartswith": "LIKE UPPER({}) || '%%'",
        "endswith": "LIKE '%%' || {}",
        "iendswith": "LIKE '%%' || UPPER({})",
    }

    Database = pg8000.dbapi
    SchemaEditorClass = DatabaseSchemaEditor
    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    # PostgreSQL backend-specific attributes.
    _named_cursor_idx = 0

    @cached_property
    def pg_version(self):
        with self.temporary_connection():
            ps = self.connection._con.parameter_statuses
            vers, *_ = ps["server_version"].split()
            idx = vers.index(".")
            return int(vers[:idx] + vers[idx + 1 :].zfill(4))

    def get_database_version(self):
        return divmod(self.pg_version, 10000)

    def get_connection_params(self):
        settings_dict = self.settings_dict
        # None may be used to connect to the default 'postgres' db
        if settings_dict["NAME"] == "" and not settings_dict.get("OPTIONS", {}).get(
            "service"
        ):
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the NAME or OPTIONS['service'] value."
            )

        if settings_dict["NAME"]:
            conn_params = {
                "database": settings_dict["NAME"],
                **settings_dict["OPTIONS"],
            }
        elif settings_dict["NAME"] is None:
            # Connect to the default 'postgres' db.
            settings_dict.get("OPTIONS", {}).pop("service", None)
            conn_params = {"database": "postgres", **settings_dict["OPTIONS"]}
        else:
            conn_params = {**settings_dict["OPTIONS"]}

        conn_params.pop("assume_role", None)
        conn_params.pop("isolation_level", None)

        if settings_dict["USER"]:
            conn_params["user"] = settings_dict["USER"]
        if settings_dict["PASSWORD"]:
            conn_params["password"] = settings_dict["PASSWORD"]
        if settings_dict["HOST"]:
            conn_params["host"] = settings_dict["HOST"]
        if settings_dict["PORT"]:
            conn_params["port"] = settings_dict["PORT"]
        return conn_params

    def get_new_connection(self, conn_params):
        return DatabaseConnection(conn_params)

    def ensure_timezone(self):
        if self.connection is None:
            return False
        conn_timezone_name = self.connection.info.get_parameter_status("TimeZone")
        timezone_name = settings.TIME_ZONE

        if settings.USE_TZ:
            tzname = timezone_name
            timestamptz_in = timestamptz_in_use_tz
        else:
            tzname = "UTC"
            timestamptz_in = timestamptz_in_no_tz

        self.connection._con.register_in_adapter(TIMESTAMPTZ, timestamptz_in)

        self.connection._con.register_in_adapter(
            TIMESTAMPTZ_ARRAY, timestamptz_array_in
        )

        if tzname and conn_timezone_name != tzname:
            self.connection._con.run(
                f"SET TIMEZONE TO {DatabaseOperations._literal(tzname)}"
            )
            return True

        return False

    def ensure_role(self):
        if self.connection is None:
            return False
        if new_role := self.settings_dict.get("OPTIONS", {}).get("assume_role"):
            sql = self.ops.compose_sql("SET ROLE %s", [new_role])
            try:
                self.connection._con.run(sql)
            except DatabaseError as e:
                err = e.args[0]
                error_code = err["C"]
                if error_code == "22023":
                    try:
                        from django.db.backends.postgresql.psycopg_any import errors

                        raise errors.InvalidParameterValue(err["M"])
                    except ImportError:
                        raise e
                else:
                    raise e

            return True
        return False

    def get_isolation_level(self):
        options = self.settings_dict["OPTIONS"]
        if "isolation_level" in options:
            isolation_level = options["isolation_level"]
            SQL_ISOLATION_LEVELS = {
                1: "READ_UNCOMMITTED",
                2: "READ_COMMITTED",
                3: "REPEATABLE_READ",
                4: "SERIALIZABLE",
            }
            try:
                return isolation_level, SQL_ISOLATION_LEVELS[isolation_level]
            except KeyError:
                raise ImproperlyConfigured(
                    f"Invalid transaction isolation level {isolation_level} specified. "
                    f"Use one of the psycopg.IsolationLevel values."
                )
        else:
            return None, None

    def init_connection_state(self):
        super().init_connection_state()
        isolation_level_int, isolation_level_sql = self.get_isolation_level()
        if isolation_level_int is not None:
            sql = (
                f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                f"{identifier(isolation_level_sql)}"
            )
            self.connection._con.run(sql)

        # Commit after setting the time zone.
        commit_tz = self.ensure_timezone()
        # Set the role on the connection. This is useful if the credential used
        # to login is not the same as the role that owns database resources. As
        # can be the case when using temporary or ephemeral credentials.
        commit_role = self.ensure_role()

        if (commit_role or commit_tz) and not self.get_autocommit():
            self.connection.commit()

    @async_unsafe
    def create_cursor(self, name=None):
        cursor = DatabaseCursor(self.connection)
        if name is not None:
            cursor = ServerSideCursor(cursor, name)

        return cursor

    def tzinfo_factory(self, offset):
        return self.timezone

    @async_unsafe
    def ensure_connection(self):
        try:
            super().ensure_connection()
        except BaseException:
            if self.connection is not None:
                self.connection.close()
            raise

    @async_unsafe
    def chunked_cursor(self):
        self._named_cursor_idx += 1
        # Get the current async task
        # Note that right now this is behind @async_unsafe, so this is
        # unreachable, but in future we'll start loosening this restriction.
        # For now, it's here so that every use of "threading" is
        # also async-compatible.
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        # Current task can be none even if the current_task call didn't error
        if current_task:
            task_ident = str(id(current_task))
        else:
            task_ident = "sync"
        # Use that and the thread ident to get a unique name
        return self._cursor(
            name="_django_curs_%d_%s_%d"
            % (
                # Avoid reusing name in other threads / tasks
                threading.current_thread().ident,
                task_ident,
                self._named_cursor_idx,
            )
        )

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            isolation_level_int, isolation_level_sql = self.get_isolation_level()
            self.connection.isolation_level = isolation_level_int
            self.connection._isolation_level_sql = isolation_level_sql
            self.connection.autocommit = autocommit

    def check_constraints(self, table_names=None):
        """
        Check constraints by setting them to immediate. Return them to deferred
        afterward.
        """
        with self.cursor() as cursor:
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            cursor.execute("SET CONSTRAINTS ALL DEFERRED")

    def is_usable(self):
        try:
            self.connection._run("SELECT 1")
            return True
        except Error:
            return False

    @contextmanager
    def _nodb_cursor(self):
        cursor = None
        try:
            with super()._nodb_cursor() as cursor:
                yield cursor
        except (DatabaseError, WrappedDatabaseError):
            if cursor is not None:
                raise
            warnings.warn(
                "Normally Django will use a connection to the 'postgres' database "
                "to avoid running initialization queries against the production "
                "database when it's not needed (for example, when running tests). "
                "Django was unable to create a connection to the 'postgres' database "
                "and will use the first PostgreSQL database instead.",
                RuntimeWarning,
            )
            for connection in connections.all():
                if (
                    connection.vendor == "postgresql"
                    and connection.settings_dict["NAME"] != "postgres"
                ):
                    conn = self.__class__(
                        {
                            **self.settings_dict,
                            "NAME": connection.settings_dict["NAME"],
                        },
                        alias=self.alias,
                    )
                    try:
                        with conn.cursor() as cursor:
                            yield cursor
                    finally:
                        conn.close()
                    break
            else:
                raise

    def make_debug_cursor(self, cursor):
        return DatabaseCursorDebug(cursor, self)


class DatabaseCursorDebug(CursorDebugWrapper):
    def copy(self, statement):
        with self.debug_sql(statement):
            return self.cursor.copy(statement)
