# ClickHouse ORM

|    |    |
| -- | -- |
| ![image](./docimage/image1.png) | ![image](./docimage/image2.png) |
| Query type supported! | Insertion and typebox type supported! |

A lightweight, type-safe ORM layer around `@clickhouse/client` for ClickHouse analytics tables.

---

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [CHBuilder](#chbuilder)
  - [Default Columns](#default-columns)
  - [Builder Methods](#builder-methods)
  - [Column Definition](#column-definition)
  - [ClickHouse Type Map](#clickhouse-type-map)
  - [Skipping Indexes](#skipping-indexes)
- [CHModel](#chmodel)
  - [findMany](#findmany)
  - [findFirst](#findfirst)
  - [insert](#insert)
  - [sync](#sync)
  - [Table-Level Key/Value Storage](#table-level-keyvalue-storage)
  - [TypeBox Integration](#typebox-integration)
- [Where Clause](#where-clause)
- [Utility Classes](#utility-classes)
- [Utility Functions](#utility-functions)

- [Real-World Example](#real-world-example)

---

## Overview

The ORM follows a **builder → model** pattern:

1. Declare a table schema with `CHBuilder`
2. Call `.build()` to produce a `CHModel`
3. Use the model to `insert`, `findMany`, or `findFirst`

Auto-migration is supported: the model compares the declared schema against the live database and applies `ALTER TABLE` statements on startup.

The client is a singleton configured from `CLICKHOUSE_URL` (default: `http://localhost:8123`) with async inserts enabled.

---

## Quick Start

```ts
import { CHBuilder } from "./setup/clickhouse";

const EventLog = new CHBuilder("EventLog")
  .withLevel()
  .withColumn({ name: "userId", type: "Int64" })
  .withColumn({ name: "message", type: "String" })
  .withTTL("timestamp + INTERVAL 30 DAY")
  .withAutoSync()
  .build();

// Insert
await EventLog.insert({ level: "INFO", userId: "42", message: "hello" });

// Query
const rows = await EventLog.findMany({
  where: [{ userId: { "=": "42" } }],
  orderBy: { timestamp: "DESC" },
  limit: 20,
});
```

---

## CHBuilder

```ts
new CHBuilder(tableName: string, loggingFunction?: (msg: string) => void)
```

Creates a new builder for `tableName`. Throws if the name contains characters other than `[a-zA-Z0-9_]`.

### Default Columns

Every table automatically receives these two columns (they are **unmigratable** — never added or removed by the migration engine):

| Column      | Type           | Default                 |
|-------------|----------------|-------------------------|
| `timestamp` | `DateTime64(3)` | `now64(3)`             |
| `eventId`   | `UUID`          | `generateUUIDv4()`     |

The underlying `CREATE TABLE` uses `MergeTree`, ordered by `(timestamp, eventId)` and partitioned by `toYYYYMM(timestamp)`.

### Builder Methods

All `with*` methods return `this` (fluent API) and put the builder into a **write-only** state — `withExtend` must be called first if you intend to use it.

> **Important: Method Chaining Required for Type Inference**
>
> Because `CHBuilder` dynamically constructs the TypeScript type of your table based on the columns you add, you **must** chain the builder methods. If you assign the builder to a variable and call methods on it sequentially, the TypeScript compiler will not update the variable's type, and you will lose all type inference for your columns.
>
> **Incorrect (Type is lost):**
> ```ts
> const builder = new CHBuilder("Users");
> builder.withColumn({ name: "age", type: "Int32" }); 
> const model = builder.build(); // 'model' will not know about the 'age' column
> ```
>
> **Correct (Type is preserved):**
> ```ts
> const model = new CHBuilder("Users")
>   .withColumn({ name: "age", type: "Int32" })
>   .build(); // 'model' correctly infers the 'age' column
> ```

---

#### `withColumn(column)`

Adds a custom column. See [Column Definition](#column-definition) for the full schema.

```ts
builder.withColumn({
  name: "userId",
  type: "Int64",
});

builder.withColumn({
  name: "content",
  type: "JSON",
  schema: TMyTypebox,            // overrides the inferred TypeScript type
});

builder.withColumn({
  name: "status",
  type: new CustomColumnType("Enum8('ACTIVE'=1,'INACTIVE'=2)"),
  queryValueType: "String",      // used in WHERE parameterisation
});
```

The TypeScript type of each field is inferred from `type` (via `ClickHouseTypeMap`) or from `schema` if provided.

---

#### `withLevel()`

Shorthand for a `level` column typed as `Enum8('DEBUG'=1,'INFO'=2,'WARN'=3,'ERROR'=4,'FATAL'=5)`, plus an automatic `set(0)` skipping index on that column.

```ts
builder.withLevel();
// Adds: level column + level_index skipping index
```

TypeScript type of `level` is `"DEBUG" | "INFO" | "WARN" | "ERROR" | "FATAL"`.

---

#### `withSkipping(skipping)`

Adds a [ClickHouse data-skipping index](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes).

```ts
builder.withSkipping({
  name: "user_id_index",
  expr: "userId",
  type: "set(0)",
});
```

`type` accepts: `"minmax"`, `"set"`, `"set(0)"`, `"bloom_filter"`, `"ngrambf_v1"`, `"tokenbf_v1"`, `"text"`, or a `CustomSkippingType` instance.

---

#### `withTTL(ttl)`

Sets the table TTL expression. Tracked in the table comment so changes are applied on the next `sync`.

```ts
builder.withTTL("timestamp + INTERVAL 1 MONTH");
```

---

#### `withExtend(target)`

Inherits all columns, skipping indexes, TTL, and logging configuration from another builder. Must be called **before** any other `with*` method.

```ts
const Base = new CHBuilder("_Base")
  .withLevel()
  .withColumn({ name: "userId", type: "Int64" });

const Derived = new CHBuilder("DerivedTable")
  .withExtend(Base)           // must be first
  .withColumn({ name: "extra", type: "String" })
  .build();
```

---

#### `withAutoSync(allowDestructive?: boolean)`

Triggers `sync()` automatically when `.build()` is called. `allowDestructive: true` also drops columns that have been removed from the definition (default: `false` — prints a warning instead).

```ts
builder.withAutoSync();           // safe migration only
builder.withAutoSync(true);       // also drops removed columns
```

---

#### `withLoggingFunction(fn)`

Overrides the logger (default: `console.log`).

```ts
builder.withLoggingFunction((msg) => myLogger.info(msg));
```

---

#### `lock()`

Makes the builder read-only without producing a model. Useful for shared base builders.

```ts
const IBase = new CHBuilder("IBase").withLevel().lock();
// IBase can now only be used with withExtend
```

---

#### `build()`

Finalises the builder and returns a `CHModel`. The builder is locked after this call.

```ts
const MyModel = new CHBuilder("MyTable").withColumn(...).build();
```

---

### Column Definition

```ts
interface Column {
  name: string;           // must match /^[a-zA-Z_][a-zA-Z0-9_]*$/
  type: keyof ClickHouseTypeMap | CustomColumnType;
  default?: string;       // SQL expression, e.g. "generateUUIDv4()"
  codec?: string;         // compression, e.g. "ZSTD(3)"
  schema?: TSchema;       // TypeBox schema — overrides inferred TS type
  queryValueType?: string; // ClickHouse type used in WHERE params, e.g. "String" for Enums
  queryValueSchema?: TSchema; // TypeBox schema for WHERE param values
  unmigratable?: boolean; // if true, migration engine ignores this column
}
```

Use `CustomColumnType` for parameterised types:

```ts
new CustomColumnType("Enum8('A'=1, 'B'=2)")
new CustomColumnType("FixedString(36)")
```

---

### ClickHouse Type Map

The following built-in type strings map to TypeScript types:

| ClickHouse Type        | TypeScript Type |
|------------------------|-----------------|
| `String`, `FixedString`, `UUID` | `string` |
| `Int8`…`Int32`, `UInt8`…`UInt32`, `Float32`, `Float64` | `number` |
| `Int64`…`UInt256`, `Decimal` | `string` (bigint serialised by client) |
| `Bool`               | `boolean` |
| `Date`, `Date32`, `DateTime`, `DateTime64`, `DateTime64(3/6/9)` | `string` |
| `JSON`               | `any` |
| `$any`               | `any` |

Use `schema` on the column to override these for `JSON` columns with known structure.

---

### Skipping Indexes

Skipping indexes are synced with the live database on every `sync()` call:

- **Added** — if declared but not in DB
- **Updated** — if `expr` or `type` changed (delete + recreate + materialize)
- **Deleted** — if in DB but not in declaration

---

## CHModel

Produced by `CHBuilder.build()`. Operates on a single ClickHouse table.

### findMany

```ts
model.findMany(query): Promise<Result[]>
```

Returns all matching rows as an array.

```ts
const rows = await MyModel.findMany({
  select: { userId: true, message: true },   // omit to select all
  where: [
    { userId: { "=": "42" } },
    { level: { "IN": ["ERROR", "FATAL"] } },
  ],
  orderBy: { timestamp: "DESC" },
  limit: 100,
  logQuery: true,   // prints the generated SQL + params to the logger
  dryRun: true,     // returns [] without hitting the database
});
```

The return type is narrowed to only the selected columns when `select` is provided.

---

### findFirst

```ts
model.findFirst(query): Promise<Result | null>
```

Same as `findMany` but automatically applies `LIMIT 1` and returns the first row or `null`.

```ts
const row = await MyModel.findFirst({
  where: [{ eventId: { "=": someUuid } }],
});
```

---

### insert

```ts
model.insert(data): Promise<void>
```

Inserts a single row. Only columns without a `default` are required; columns with defaults may be omitted.

```ts
await MyModel.insert({
  userId: "42",
  level: "INFO",
  message: "user logged in",
  // timestamp and eventId are omitted — they have server-side defaults
});
```

Uses `JSONEachRow` format with async insert enabled on the client.

---

### sync

```ts
model.sync(allowDestructive?: boolean): Promise<this>
```

Manually triggers migration. Creates the table if it does not exist, then:

1. Adds or modifies columns that differ from the declaration
2. Drops columns not in the declaration (only when `allowDestructive: true`)
3. Adds, updates, or removes skipping indexes
4. Updates the table TTL

---

### Table-Level Key/Value Storage

Arbitrary string values can be stored inside the table comment. Useful for application-level metadata that must survive restarts. Reads are cached; writes and deletes are slow (involve `ALTER TABLE`).

```ts
// Write
await model.setTableValue("myKey", "myValue");

// Read (cached)
const value = await model.getTableValue("myKey"); // string | undefined

// Delete
const deleted = await model.deleteTableValue("myKey"); // boolean
```

Keys must match `/^[a-zA-Z_][a-zA-Z0-9_]*$/`.

> The TTL is also tracked this way internally under the key `#last_ttl`.

---

### TypeBox Integration

`CHModel.asTypebox()` generates a TypeBox `TObject` schema that mirrors the model's column structure. This makes it straightforward to share the schema between the ClickHouse model and API response validation (e.g., Fastify route schemas).

```ts
model.asTypebox(options?: ObjectOptions): TObject
```

For each valid column the schema is resolved in priority order:

1. The column's explicit `schema` field (set via `withColumn({ schema: T })`)
2. The built-in TypeBox equivalent from `ClickHouseTypeboxMap` (when `type` is a known string key)
3. `Type.Any()` as a fallback (e.g., for `CustomColumnType` columns with no `schema`)

#### ClickHouse → TypeBox type mapping

| ClickHouse Type | TypeBox Schema |
|---|---|
| `String`, `FixedString`, `UUID` | `Type.String()` |
| `Int8`…`Int32`, `UInt8`…`UInt32`, `Float32`, `Float64` | `Type.Number()` |
| `Int64`…`UInt256`, `Decimal` | `Type.String()` (bigint serialised as string by `@clickhouse/client`) |
| `Bool` | `Type.Boolean()` |
| `Date`, `Date32`, `DateTime`, `DateTime64`, `DateTime64(3/6/9)` | `Type.String()` |
| `JSON` | `Type.Any()` |
| `$any` | `Type.Any()` |
| `CustomColumnType` (no `schema`) | `Type.Any()` |

The optional `options` argument is forwarded to `Type.Object()` — useful for setting `$id`, `title`, or other JSON Schema keywords.

#### Example

```ts
const EventLog = new CHBuilder("EventLog")
  .withLevel()
  .withColumn({ name: "userId",  type: "Int64" })
  .withColumn({
    name:   "content",
    type:   "JSON",
    schema: UserModel.TPersonalInformation, // explicit TypeBox schema
  })
  .build();

// Produces TObject:
//   timestamp : Type.String()
//   eventId   : Type.String()
//   level     : Type.Union([Type.Literal("DEBUG"), ...])  ← from withLevel()
//   userId    : Type.String()                             ← Int64 → string
//   content   : UserModel.TPersonalInformation            ← explicit schema wins
const TEventLog = EventLog.asTypebox({ $id: "EventLog", title: "#EventLog" });
```

Use `TEventLog` directly as a Fastify `response` schema or register it with `instance.addSchema()`.

---

## Where Clause

The `where` array accepts a mix of:

### Condition objects

```ts
{ columnName: { operator: value } }
```

Multiple columns or operators within one object are combined with `AND`. Adjacent condition objects without a separator are also combined with `AND`.

```ts
where: [
  { userId: { "=": "42" } },
  { level: { "IN": ["ERROR", "FATAL"] } },
  { timestamp: { ">=": "2024-01-01", "<": "2025-01-01" } },
]
// => (userId = 42) AND (level IN ('ERROR','FATAL')) AND (timestamp >= '...' AND timestamp < '...')
```

### Supported operators

| Operator    | Value type          |
|-------------|---------------------|
| `=`         | scalar              |
| `!=`        | scalar              |
| `>`         | scalar              |
| `>=`        | scalar              |
| `<`         | scalar              |
| `<=`        | scalar              |
| `LIKE`      | string              |
| `IN`        | array of scalars    |
| `BETWEEN`   | `[low, high]` tuple |

### Logical grouping strings

```ts
where: ["(", { a: { "=": 1 } }, "OR", { b: { "=": 2 } }, ")"]
// => (a = 1 OR b = 2)
```

Accepted string tokens: `"("`, `")"`, `"AND"`, `"OR"`.

### Raw values and queries

```ts
import { TypedValue, UnsafeRawQuery } from "./setup/clickhouse";

where: [
  new TypedValue("UInt32", 99),          // typed parameter
  new UnsafeRawQuery("isNotNull(field)"), // raw SQL — use with caution
]
```

---

## Utility Classes

### `CustomColumnType`

Wraps a raw ClickHouse type string for complex types that are not in `ClickHouseTypeMap`.

```ts
new CustomColumnType("Enum8('ACTIVE'=1, 'INACTIVE'=2)")
new CustomColumnType("Array(String)")
```

### `CustomSkippingType`

Wraps a skipping index type with parameters.

```ts
new CustomSkippingType("set(100)")
new CustomSkippingType("ngrambf_v1(4, 1024, 1, 0)")
```

### `UnsafeRawQuery`

Injects a raw SQL fragment into a `where` clause. No escaping is applied — ensure values come from trusted sources.

```ts
new UnsafeRawQuery("hasToken(message, 'error')")
```

### `TypedValue`

Injects a parameterised value with an explicit ClickHouse type into a `where` clause.

```ts
new TypedValue("UInt64", "18446744073709551615")
```

---

### Utility Functions

The following utility functions are used internally by the ORM but are also exported for application-level use:

#### `getClient()`
Returns the singleton instance of the ClickHouse client. It is configured via the `CLICKHOUSE_URL` environment variable (defaults to `http://localhost:8123`) and has async inserts enabled by default. Useful for executing raw SQL queries or complex aggregations that are not covered by the ORM model.

#### `getCreateTableSqlFor(tableName: string)`
Fetches the raw `SHOW CREATE TABLE` SQL string directly from the database. This is particularly useful for debugging schema issues or verifying that migrations (`sync()`) have been applied as expected.

#### `verifyName(name: string)`
Validates whether a given string is a safe and valid ClickHouse identifier (such as a table or column name). It ensures the name starts with a letter or underscore, followed only by alphanumeric characters or underscores (`/^[a-zA-Z_][a-zA-Z0-9_]*$/`). Useful for sanitizing dynamic inputs to prevent query errors.

---

## Real-World Example

```ts
// Shared base (locked — not built into a model directly)
const IUserActionLog = new CHBuilder("IUserActionLog")
  .withLevel()                                   // Enum8 level + index
  .withColumn({ name: "userId", type: "Int64" })
  .withColumn({ name: "sessionToken", type: "String" })
  .withAutoSync()                                // auto-migrate on build
  .lock();                                       // read-only base

// Table 1: personal information changes
export const UserPersonalInformationUpdated = new CHBuilder("UserPersonalInformationUpdated")
  .withExtend(IUserActionLog)                    // inherit base fields
  .withColumn({
    name: "content",
    type: "JSON",
    schema: UserModel.TPersonalInformation,      // typed JSON via TypeBox
  })
  .build();
```

Each resulting table has columns: `timestamp`, `eventId`, `level`, `userId`, `sessionToken`, plus the table-specific column(s).

Usage:

```ts
await UserLogging.UserPersonalInformationUpdated.insert({
  level: "INFO",
  userId: user.id,
  sessionToken: token,
  content: UserModel.packPersonalInformation(info),
});

const recent = await UserLogging.UserPersonalInformationUpdated.findMany({
  where: [{ userId: { "=": user.id } }],
  orderBy: { timestamp: "DESC" },
  limit: 10,
});
```
