import { createClient } from "@clickhouse/client";
import type { NodeClickHouseClient } from "@clickhouse/client/dist/client";
import { type Static, type TSchema } from "@sinclair/typebox";

/** sql: 유저 사용자 지정 Query */
export class UnsafeRawQuery {
  private _query: string;
  constructor(rawQuery: string) {
    this._query = rawQuery;
  }
  public get query(): string {
    return this._query;
  }
}

/** sql: 유저 사용자 지정 값 */
export class TypedValue {
  private _type: string;
  private _value: any;
  constructor(type: string, value: any) {
    this._type = type;
    this._value = value;
  }
  public get type(): string {
    return this._type;
  }
  public get value(): string {
    return this._value;
  }
}

// TODO: improve this
let client: NodeClickHouseClient | undefined;
/** create singleton client */
export function getClient() {
  return (client ??= createClient({
    url: process.env.CLICKHOUSE_URL || "http://localhost:8123",
  }));
}

/** 테이블에 대한 db 내 상태를 반환합니다 */
export async function getCreateTableSqlFor(tableName: string): Promise<string> {
  try {
    const result = await getClient().query({
      query: `SHOW CREATE TABLE ${tableName}`,
      format: "JSONEachRow",
    });

    // 클릭하우스는 'statement'라는 키 값으로 전체 구조 SQL을 뱉어줘~
    const data = (await result.json()) as { statement: string }[];

    if (data.length > 0) {
      return data[0]!.statement;
    }

    return "undefined";
  } catch (error) {
    return `failed to fetch table sql: ${error}`;
  }
}

// #region Migration
class CHMigration {
  private skippingList: CHBuilder.Skipping[];
  private columnList: CHBuilder.Column[];
  private tableName: string;
  private loggingFunction: (msg: string) => void;

  public constructor(
    skippingList: CHBuilder.Skipping[],
    columnList: CHBuilder.Column[],
    tableName: string,
    loggingFunction: CHBuilder["loggingFunction"] = console.log.bind(console),
  ) {
    this.tableName = tableName;
    this.columnList = columnList;
    this.skippingList = skippingList;
    this.loggingFunction = loggingFunction;
  }

  public log(msg: string) {
    this.loggingFunction(
      `ClickhouseModel(${this.tableName}) Migration: ${msg}`,
    );
  }

  private async command(query: string) {
    this.log(`execute '${query}`);
    await getClient().command({ query });
  }

  // Manage skippings
  private async deleteSkipping(
    skipping: CHBuilder.Skipping,
  ): Promise<undefined> {
    await this.command(
      `ALTER TABLE ${this.tableName} DROP INDEX ${skipping.name}`,
    );
  }
  private async createSkipping(
    skipping: CHBuilder.Skipping,
  ): Promise<undefined> {
    await this.command(
      `ALTER TABLE ${this.tableName} ADD INDEX ${skipping.name} ${skipping.expr} TYPE ${skipping.type}`,
    );
    await this.command(
      `ALTER TABLE ${this.tableName} MATERIALIZE INDEX ${skipping}`,
    );
  }
  private async syncSkipping(): Promise<boolean> {
    // Get skippings from database
    let updated = false;
    const query = await getClient().query({
      query: `SELECT name, expr, type FROM system.data_skipping_indices WHERE table = '${this.tableName}'`,
      format: "JSONEachRow",
    });
    const dbList = (await query.json()) as CHBuilder.Skipping[];

    // Compare from database's skippings
    for (const dbItem of dbList) {
      const matched = this.skippingList.find((i) => i.name == dbItem.name);

      // Should be deleted
      if (!matched) {
        this.log(`update skipping '${dbItem.name}'`);
        await this.deleteSkipping(dbItem);
        updated = true;
        continue;
      }

      // Should be updated
      if (matched.expr != dbItem.expr || matched.type != dbItem.type) {
        this.log(`update skipping '${matched.name}'`);
        await this.deleteSkipping(matched);
        await this.createSkipping(matched);
        updated = true;
      }
    }

    // Should be created
    for (const item of this.skippingList) {
      if (dbList.find((i) => i.name == item.name)) continue;
      this.log(`create skipping '${item.name}'`);
      await this.createSkipping(item);
      updated = true;
    }

    return updated;
  }

  // Manage columns
  private static columnNormalize(
    base: CHBuilder.Column,
  ): CHMigration.NormalizedColumn {
    return {
      name: base.name.trim(),
      type: base.type.trim(),
      default: base.default?.trim() ?? "",
      codec: base.codec?.toUpperCase()?.trim() ?? "",
      ttl: base.ttl?.trim() ?? "",
    };
  }
  private static dbColumnNormalize(
    base: CHMigration.DBColumn,
  ): CHMigration.NormalizedColumn {
    return {
      name: base.name.trim(),
      type: base.type.trim(),
      default: base.default_expression?.trim() ?? "",
      codec: base.compression_codec?.toUpperCase()?.trim() ?? "",
      ttl: base.ttl_expression?.trim() ?? "",
    };
  }
  private static diffColumn(
    define: CHMigration.NormalizedColumn,
    db: CHMigration.NormalizedColumn,
  ): boolean {
    // Type is different
    if (define.type != db.type) return true;

    // Default is different
    if (define.default != db.default) return true;

    // ttl is different
    if (define.ttl != db.ttl) return true;

    // codec is different(empty not match)
    const dbCodecIsEmpty =
      db.codec == "" || db.codec.includes("NONE") || db.codec.includes("LZ4");
    const defineCodecIsEmpty = define.codec == "" || define.codec == "NONE";
    if (defineCodecIsEmpty != dbCodecIsEmpty) return true;

    // codec is different(content is not match)
    if (!defineCodecIsEmpty && !db.codec.includes(define.codec)) return true;

    return false;
  }
  private async syncColumn() {
    let updated = false;
    const query = await getClient().query({
      query: `SELECT name, type, default_expression, compression_codec, ttl_expression 
              FROM system.columns WHERE table = '${this.tableName}' AND database = currentDatabase()`,
      format: "JSONEachRow",
    });
    const dbList = (await query.json()) as CHMigration.DBColumn[];

    for (const item of this.columnList) {
      if (item.unmigratable) continue;
      const dbItem = dbList.find((i) => i.name === item.name);

      // Construct sql parts
      const defaultPart = item.default ? `DEFAULT ${item.default}` : "";
      const codecPart = item.codec ? `CODEC(${item.codec})` : "";
      const ttlPart = item.ttl ? `TTL ${item.ttl}` : "";
      const fullDefine =
        `${item.type} ${defaultPart} ${codecPart} ${ttlPart}`.trim();

      // Create new column
      if (!dbItem) {
        this.log(`create new column '${item.name}'`);
        await this.command(
          `ALTER TABLE ${this.tableName} ADD COLUMN ${item.name} ${fullDefine}`,
        );
        updated = true;
        continue;
      }

      // Update column
      if (
        CHMigration.diffColumn(
          CHMigration.columnNormalize(item),
          CHMigration.dbColumnNormalize(dbItem),
        )
      ) {
        this.log(`update existing column ${item.name}`);
        await this.command(
          `ALTER TABLE ${this.tableName} MODIFY COLUMN ${item.name} ${fullDefine}`,
        );
        updated = true;
      }
    }

    // Show column should be deleted
    for (const dbItem of dbList) {
      if (this.columnList.find((i) => i.name == dbItem.name)) continue;
      const dropQuery = `ALTER TABLE ${this.tableName} DROP COLUMN ${dbItem.name};`;
      this.log(
        `column '${dbItem.name}' should be deleted! destructive migration must be done by user.\n QUERY: ${dropQuery}`,
      );
      updated = true;
    }

    return updated;
  }

  // Create / manage table
  public async sync(): Promise<undefined> {
    try {
      await getClient().command({
        query: `
          CREATE TABLE IF NOT EXISTS ${this.tableName} (
            timestamp DateTime64(3)
            eventId UUID DEFAULT generateUUIDv4()
            level Enum8('DEBUG'=1, 'INFO'=2, 'WARN'=3, 'ERROR'=4, 'FATAL'=5)
          )
          ENGINE = MergeTree()
          ORDER BY (timestamp, event_id)
          PARTITION BY toYYYYMM(timestamp)
        `,
      });
      let updated = false;
      if (await this.syncColumn()) updated = true;
      if (await this.syncSkipping()) updated = true;

      if (updated) {
        const sql = await getCreateTableSqlFor(this.tableName);
        this.log(`table updated. SQL:\n${sql}`);
      }
    } catch (e) {
      const sql = await getCreateTableSqlFor(this.tableName);
      this.log(`table update failed. ERROR:\n${e}\nSQL:\n${sql}`);
    }
  }
}
namespace CHMigration {
  // DB 에서 읽은 컬럼
  export interface DBColumn {
    name: string;
    type: string;
    default_expression?: string;
    compression_codec?: string;
    ttl_expression?: string;
  }
  // DB 와의 비교를 위한 선언과 DB 컬럼의 중간 형태
  export interface NormalizedColumn {
    name: string;
    type: string;
    default: string;
    codec: string;
    ttl: string;
  }
}
// #endregion Migration

// #region Query
export class CHQuery {
  private paramList: any[];
  private columns: CHBuilder.Column[];
  private buf: string[];

  public constructor(columns: CHBuilder.Column[]) {
    this.paramList = [];
    this.columns = columns;
    this.buf = [];
  }

  // 리스트인 params 를 쿼리에 사용 가능하도록 {v1: value} 형태로 반환합니다
  public mapParamList(): { [key: string]: any } {
    return Object.fromEntries(
      this.paramList.map((value, index) => [`v${index}`, value]),
    );
  }

  // 전체를 렌더링합니다
  public renderQuery(): string {
    return this.buf.join("");
  }

  // 키가 가지는 타입에 맞는 인자를 추가하고 그에 맞는 쿼리 스트링을 반환
  public pushParam(key: string, value: any): string {
    const define = this.columns.find((i) => i.name == key);
    if (!define) throw Error(`Column '${key}' not found from definition`);
    return this.pushTypedParam(define.queryValueType ?? define.type, value);
  }

  // 인자를 추가하고 그에 맞는 쿼리 스트링을 반환
  public pushTypedParam(type: string, value: any): string {
    const query = `{v${this.paramList.length}:${type}}`;
    this.paramList.push(value);
    return query;
  }

  // 쿼리 버퍼에 쿼리 스트링 푸시
  private push(query: string): this {
    this.buf.push(query);
    return this;
  }

  // 오퍼레이터를 쿼리 버퍼에 푸시
  public pushOperator(key: string, operator: string, value: any) {
    this.push(`(${key} ${operator} `);
    if (operator == "IN") {
      this.push("(");
      let first = true;
      for (const element of value) {
        if (!first) {
          this.push(",");
        }
        first = false;

        this.push(this.pushParam(key, element));
      }
      this.push(")");
    } else if (operator == "BETWEEN") {
      this.push(this.pushParam(key, value[0]))
        .push(" AND ")
        .push(this.pushParam(key, value[1]));
    } else {
      this.push(this.pushParam(key, value));
    }
    this.push(")");
  }

  // SELECT 문을 렌더
  public pushSelectClause<Fields extends CHBuilder.FieldsType>(
    query: CHQuery.SelectQuery<Fields>,
  ): this {
    let items = "*";
    if (query.select) {
      items = Object.entries(query.select)
        .filter((i) => i[1])
        .map((i) => i[0])
        .join(", ");
    }
    this.push(`SELECT ${items}\n`);
    return this;
  }

  // FROM 문을 렌더
  public pushFromClause(tableName: string): this {
    this.push(`FROM ${tableName}\n`);
    return this;
  }

  // WHERE 문을 렌더
  public pushWhereClause<Fields extends CHBuilder.FieldsType>(
    query: CHQuery.WhereQuery<Fields>,
  ): this {
    if (!query.where) return this;
    if (!Object.entries(query.where).length) return this;
    this.push("WHERE ");

    let lastStatementIsCondition = false;
    for (const statement of query.where) {
      // "(", "AND", "OR", ")"
      if (typeof statement === "string") {
        lastStatementIsCondition = false;
        if (statement == "(" || statement == ")") {
          this.push(statement);
        } else {
          this.push(` ${statement} `);
        }
        continue;
      }

      // Push user defined value
      if (statement instanceof TypedValue) {
        lastStatementIsCondition = false;
        this.pushTypedParam(statement.type, statement.value);
        continue;
      }

      // Push user defined raw query
      if (statement instanceof UnsafeRawQuery) {
        lastStatementIsCondition = false;
        this.push(statement.query);
        continue;
      }

      // Push AND between two condition
      if (lastStatementIsCondition) {
        this.push(" AND ");
      }
      lastStatementIsCondition = true;

      // { key: { op: vaalue } }
      this.push("(");
      let firstCondition = true;
      for (const [key, conditionMap] of Object.entries(statement as any)) {
        // Push AND between condition
        if (!firstCondition) {
          this.push(" AND ");
        }
        firstCondition = false;

        let firstOp = true;
        for (const [operator, value] of Object.entries(conditionMap as any)) {
          // Push AND between operator
          if (!firstOp) {
            this.push(" AND ");
          }
          firstOp = false;
          this.pushOperator(key, operator, value);
        }
      }
      this.push(")");
    }
    this.push("\n");

    return this;
  }

  // LIMIT 문을 렌더
  public pushLimitClause(query: CHQuery.LimitQuery): this {
    if (query.limit) this.push(`LIMIT ${query.limit}\n`);
    return this;
  }

  // ORDER BY 문을 렌더
  public pushOrderByClause<Fields extends CHBuilder.FieldsType>(
    query: CHQuery.OrderByQuery<Fields>,
  ): this {
    if (!query.orderBy) return this;

    const items = Object.entries(query.orderBy)
      .map(([name, direction]) => `${name as string} ${direction}`)
      .join(", ");
    if (items == "") return this;

    this.push("ORDER BY ");
    this.push(items);
    this.push("\n");

    return this;
  }
}
export namespace CHQuery {
  export type FieldsExtract<T> = T extends CHBuilder<infer U> ? U : never;
  export type FieldsJson<T, M = FieldsExtract<T>> = {
    [K in keyof M]: M[K];
  };
  export type OnlyTrue<T> = {
    [K in keyof T as T[K] extends true ? K : never]: T[K];
  };

  /**
   * Where 안에 들어갈 수 있는 연산자 모음
   * 각 연산자에 따른 피연산자가 달라집니다
   */
  // prettier-ignore
  export type WhereOperator<Type> = {
    ">"?: Type,
    ">="?: Type,
    "<"?: Type,
    "<="?: Type,
    "="?: Type,
    "!="?: Type,
    "LIKE"?: Type,
    "IN"?: Type[],
    "BETWEEN"?: [Type, Type],
  }

  // Query type
  // prettier-ignore
  export type WhereQuery<Fields extends CHBuilder.FieldsType> = {
    where?: (
      ("(" | ")" | "AND" | "OR") |
      TypedValue |
      UnsafeRawQuery |
      { [K in keyof Fields]?: WhereOperator<Fields[K]["query"]>; }
    )[] | undefined;
  };
  export type SelectQuery<Fields extends CHBuilder.FieldsType> = {
    select?: { [K in keyof Fields]?: boolean } | undefined;
  };
  export type OrderByQuery<Fields extends CHBuilder.FieldsType> = {
    orderBy?: { [K in keyof Fields]?: "DESC" | "ASC" } | undefined;
  };
  export type LimitQuery = {
    limit?: number | undefined;
  };

  // Result type
  export type Result<
    Fields extends CHBuilder.FieldsType,
    Query extends SelectQuery<Fields>,
  > = Query["select"] extends {}
    ? Pick<Fields, keyof CHQuery.OnlyTrue<Query["select"]>>
    : Fields;
}
// #endregion Query

// #region Builder
export class CHBuilder<
  Fields extends CHBuilder.FieldsType = CHBuilder.DefaultFields,
> {
  private skippingList: CHBuilder.Skipping[];
  private columnList: CHBuilder.Column[];
  private tableName: string;
  private loggingFunction: (msg: string) => void;

  public constructor(
    tableName: string,
    loggingFunction: CHBuilder["loggingFunction"] = console.log.bind(console),
  ) {
    this.tableName = tableName;
    this.skippingList = [];
    this.columnList = [...CHBuilder.DefaultColumns];
    this.loggingFunction = loggingFunction;
  }

  public build(): CHModel<Fields> {
    return new CHModel(
      this.skippingList,
      this.columnList,
      this.tableName,
      this.loggingFunction,
    );
  }

  public withLoggingFunction(func: CHBuilder["loggingFunction"]): this {
    this.loggingFunction = func;
    return this;
  }

  public withColumn<
    Name extends string,
    Type extends keyof CHBuilder.ClickHouseTypeMap | string,
    Schema extends TSchema | undefined = undefined,
    QueryValueSchema extends TSchema | undefined = undefined,
  >(
    column: CHBuilder.Column<Name, Type, Schema, QueryValueSchema>,
  ): CHBuilder<
    Fields & {
      [K in Name]: {
        selected: Schema extends TSchema
          ? Static<Schema>
          : Type extends keyof CHBuilder.ClickHouseTypeMap
          ? CHBuilder.ClickHouseTypeMap[Type]
          : never;
        query: QueryValueSchema extends TSchema
          ? Static<QueryValueSchema>
          : Type extends keyof CHBuilder.ClickHouseTypeMap
          ? CHBuilder.ClickHouseTypeMap[Type]
          : never;
      };
    }
  > {
    this.columnList.push(column as CHBuilder.Column);
    return this as any;
  }

  public withSkipping(skipping: CHBuilder.Skipping): this {
    this.skippingList.push(skipping);
    return this;
  }
}
export namespace CHBuilder {
  // Field is type holder for ClickhouseModel
  export type FieldsType = { [key: string]: { selected: any; query: any } };

  // Define default columns and default fields
  export const DefaultColumns = [
    {
      name: "timestamp",
      type: "DateTime64(3)",
      unmigratable: true,
    },
    {
      name: "eventId",
      type: "UUID",
      default: "generateUUIDv4()",
      unmigratable: true,
    },
    {
      name: "level",
      type: "Enum8('DEBUG'=1, 'INFO'=2, 'WARN'=3, 'ERROR'=4, 'FATAL'=5)",
      queryValueType: "String",
      unmigratable: true,
    },
  ];
  export type LogLevel = "DEBUG" | "INFO" | "WARN" | "ERROR" | "FATAL";
  export type DefaultFields = {
    timestamp: { selected: string; query: string };
    eventId: { selected: string; query: string };
    level: { selected: LogLevel; query: LogLevel };
  } & { [key: string]: { selected: any; query: any } };

  // 스킵 색인 필드 선언
  export interface Skipping {
    name: string;
    expr: string;
    type: string;
  }

  // 컬럼 선언
  export interface Column<
    Name extends string = string,
    Type extends keyof ClickHouseTypeMap | string =
      | keyof ClickHouseTypeMap
      | string,
    Schema extends TSchema | undefined = undefined,
    QueryValueSchema extends TSchema | undefined = undefined,
  > {
    name: Name;
    type: Type;
    default?: string; // 예: "generateUUIDv4()"
    codec?: string; // 예: "ZSTD(3)"
    ttl?: string; // 예: "timestamp + INTERVAL 30 DAY"
    /**
     * 만약 JSON 과 같은 타입에 대해 타입을 지정하려면 TypeBox 를 사용하세요
     * 이 값이 주어지면 모든 기초 타입을 덮어씁니다.
     * */
    schema?: Schema;
    /**
     * 마이그레이션으로 관리되지 않는 컬럼입니다.
     */
    unmigratable?: boolean;
    /**
     * 쿼리 시 오퍼레이터의 피연산자 대상의 clickhouse 타입입니다. 내부적으로 `field = {v1:queryValueType}` 과 같이 사용됩니다
     * Enum 과 같이 Type 필드가 복잡한 경우 이 값을 "string" 으로 두는것을 고려하세요.
     */
    queryValueType?: string;
    /**
     * 쿼리 시 클라이언트에 전달하는 값의 타입입니다.
     */
    queryValueSchema?: QueryValueSchema;
  }

  // 타입 맵 (Clickhouse type => typescript client type)
  export type ClickHouseTypeMap = {
    // 문자열 및 식별자
    String: string;
    FixedString: string;
    UUID: string;

    // 숫자 (JS number 범위 내)
    Int8: number;
    UInt8: number;
    Int16: number;
    UInt16: number;
    Int32: number;
    UInt32: number;
    Float32: number;
    Float64: number;

    // 큰 숫자 (@clickhouse/client 기본값은 string)
    Int64: string;
    UInt64: string;
    Int128: string;
    UInt128: string;
    Int256: string;
    UInt256: string;
    Decimal: string;

    // 논리값
    Bool: boolean;

    Date: string;
    Date32: string;
    DateTime: string;
    DateTime64: string;
    "DateTime64(3)": string;
    "DateTime64(6)": string;
    "DateTime64(9)": string;

    JSON: any;

    $any: any;
  };
}
// #endregion Builder

// #region Model
export class CHModel<Fields extends CHBuilder.FieldsType> {
  private skippingList: CHBuilder.Skipping[];
  private columnList: CHBuilder.Column[];
  private tableName: string;
  private loggingFunction: (msg: string) => void;

  constructor(
    skippingList: CHBuilder["skippingList"],
    columnList: CHBuilder["columnList"],
    tableName: CHBuilder["tableName"],
    loggingFunction: CHBuilder["loggingFunction"] = console.log.bind(console),
  ) {
    this.skippingList = skippingList;
    this.columnList = columnList;
    this.tableName = tableName;
    this.loggingFunction = loggingFunction;
  }

  public async sync(): Promise<this> {
    const migrationHandle = new CHMigration(
      this.skippingList,
      this.columnList,
      this.tableName,
      this.loggingFunction,
    );
    await migrationHandle.sync();

    return this;
  }

  public async findMany<
    Query extends {
      /**
       * If the two conditions are adjacent without any intervening characters, an AND statement is inserted between them.
       *
       * Additionally, conditional statements are automatically enclosed in parentheses; if multiple conditions exist within a single conditional statement, they are treated as being combined using the AND operator.
       *
       * You can put raw query by `new UnsafeRawQuery('sql')` and value by `new TypedValue('Type', value)`
       *
       * EX: [ { a: {"=": true}, b: {"=": true} }, { c: {"=": true} } ]
       *     => (a = true AND b = true) AND (c = true)
       * */
      where?: CHQuery.WhereQuery<Fields>["where"];
      /**
       * Select only specific columns. Select all columns if leave it undefined
       */
      select?: CHQuery.SelectQuery<Fields>["select"];
      orderBy?: CHQuery.OrderByQuery<Fields>["orderBy"];
      limit?: CHQuery.LimitQuery["limit"];
    },
    Result = CHQuery.Result<Fields, Query>,
  >(query: Query): Promise<Result[]> {
    const builder = new CHQuery(this.columnList)
      .pushSelectClause(query)
      .pushFromClause(this.tableName)
      .pushWhereClause(query)
      .pushOrderByClause(query)
      .pushLimitClause(query);

    const resultSet = await getClient().query({
      query: builder.renderQuery(),
      query_params: builder.mapParamList(),
      format: "JSONEachRow",
    });

    return (await resultSet.json<Result>()) ?? [];
  }

  public async findFirst<
    Query extends {
      /**
       * If the two conditions are adjacent without any intervening characters, an AND statement is inserted between them.
       *
       * Additionally, conditional statements are automatically enclosed in parentheses; if multiple conditions exist within a single conditional statement, they are treated as being combined using the AND operator.
       *
       * You can put raw query by `new UnsafeRawQuery('sql')` and value by `new TypedValue('Type', value)`
       *
       * EX: [ { a: {"=": true}, b: {"=": true} }, { c: {"=": true} } ]
       *     => (a = true AND b = true) AND (c = true)
       * */
      where?: CHQuery.WhereQuery<Fields>["where"];
      /**
       * Select only specific columns. Select all columns if leave it undefined
       */
      select?: CHQuery.SelectQuery<Fields>["select"];
      orderBy?: CHQuery.OrderByQuery<Fields>["orderBy"];
    },
    Result = CHQuery.Result<Fields, Query>,
  >(query: Query): Promise<Result | null> {
    const builder = new CHQuery(this.columnList)
      .pushSelectClause(query)
      .pushFromClause(this.tableName)
      .pushWhereClause(query)
      .pushOrderByClause(query)
      .pushLimitClause({ limit: 1 });

    const resultSet = await getClient().query({
      query: builder.renderQuery(),
      query_params: builder.mapParamList(),
      format: "JSONEachRow",
    });

    return (await resultSet.json<Result>())?.[0] ?? null;
  }
}
// #endregion Model
