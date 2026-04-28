import { createClient } from "@clickhouse/client";
import type { NodeClickHouseClient } from "@clickhouse/client/dist/client";
import { Type, type Static, type TSchema } from "@sinclair/typebox";
import util from "node:util";

// #region default
/** sql: 유저 사용자 지정 Query */
export class UnsafeRawQuery {
  private _query: string;
  public constructor(rawQuery: string) {
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
  public constructor(type: string, value: any) {
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

/**
 * 사용자 지정 칼럼 타입
 * 기본 타입에 인자를 부여하려는 경우 사용하세요
 * 예: new CustomColumnType("Enum8('APPLE'=1, 'BANANA'=2)")
 */
export class CustomColumnType {
  private type: string;
  public constructor(type: string) {
    this.type = type.trim();
  }
  toString(): string {
    return this.type;
  }
}

/**
 * 사용자 지정 스킵 인덱스 타입.
 * 기본 스킵 인덱스 타입에서 인자를 부여하려는 경우 사용하세요
 * 예: new CustomSkippingType("set(0)")
 * */
export class CustomSkippingType {
  private type: string;
  public constructor(type: string) {
    this.type = type.trim();
  }
  toString(): string {
    return this.type;
  }
}

// TODO: improve this
let client: NodeClickHouseClient | undefined;
/** create singleton client */
export function getClient() {
  return (client ??= createClient({
    url: process.env.CLICKHOUSE_URL || "http://localhost:8123",
    clickhouse_settings: {
      // 비동기 삽입 활성화
      async_insert: 1,
      // 서버의 메모리 버퍼에 저장될 때까지 기다릴지 여부 (1: 대기, 0: 즉시 응답)
      wait_for_async_insert: 1,
    },
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

// 데이터베이스의 컬럼 명 또는 테이블 명이 유효한지 검사합니다.
export function verifyName(name: string): boolean {
  if (!name.length) {
    return false;
  }
  if (!name.match(/^[a-zA-Z_][a-zA-Z0-9_]*$/)) {
    return false;
  }
  return true;
}

function normalizeForDiff<Content extends string | undefined>(
  content: Content,
): Content extends string
  ? Content extends undefined
    ? string | undefined
    : string
  : undefined {
  return content?.normalize()?.replace(/\s/g, "") as any;
}
// #endregion default

// #region Comment
/** 테이블의 코멘트를 통해 마이그레이션과 관련된 데이터를 저장합니다. */
export class CHTableComment {
  private tableName: string;
  private cachedComment?: string;
  public constructor(tableName: string) {
    this.tableName = tableName;
  }

  // Extract comment from database
  public async getComment(): Promise<string> {
    if (this.cachedComment) return this.cachedComment;

    const query = await getClient().query({
      query: `SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = {tableName:String}`,
      format: "JSONEachRow",
      query_params: {
        tableName: this.tableName,
      },
    });
    const result = (await query.json()) as { comment: string }[];
    this.cachedComment = result[0]?.comment ?? "";

    return this.cachedComment;
  }

  // Update database comment
  public async setComment(newComment: string) {
    const safeComment = newComment
      .replace(/\\/g, "\\\\")
      .replace(/'/g, "\\'")
      .replace(/\n/g, "\\n")
      .replace(/\t/g, "\\t")
      .replace(/\r/g, "\\r");
    await getClient().query({
      query: `ALTER TABLE ${this.tableName} MODIFY COMMENT '${safeComment}'`,
    });
    this.cachedComment = newComment;
  }

  // Content => '%%HEAD:key%%content%%TAIL:key%%'
  private static getHeadTailTag(key: string): [string, string] {
    return [`%%HEAD:${key}%%`, `%%TAIL:${key}%%`];
  }
  private static getRegex(key: string, all: boolean = false): RegExp {
    const [head, tail] = CHTableComment.getHeadTailTag(key);
    return new RegExp(`${head}(.*?)${tail}`, all ? "g" : undefined);
  }

  // Escape value (\ => \\, % => \%)
  private static escape(value: string): string {
    return value.replace(/\\/g, "\\\\").replace(/\%/g, "\\%");
  }
  private static unescape(escaped: string): string {
    return escaped.replace(/\\([\\\%])/g, (_, matched) => {
      if (matched == "%") return "%";
      return "\\";
    });
  }

  public async getValue(key: string): Promise<string | undefined> {
    const comment = await this.getComment();
    const match = comment.match(CHTableComment.getRegex(key));

    if (!match) return undefined;
    return CHTableComment.unescape(match[1] as string);
  }

  public async setValue(key: string, value: string) {
    const [head, tail] = CHTableComment.getHeadTailTag(key);
    const comment = await this.getComment();
    const regex = CHTableComment.getRegex(key);

    const escaped = CHTableComment.escape(value);
    const newEntry = `${head}${escaped}${tail}`;

    let newComment: string;
    if (!comment.match(regex)) {
      newComment = comment + newEntry;
    } else {
      newComment = comment.replace(regex, newEntry);
    }

    if (newComment != comment) {
      await this.setComment(newComment);
    }
  }

  public async deleteValue(key: string) {
    const comment = await this.getComment();
    const newComment = comment.replaceAll(
      CHTableComment.getRegex(key, true),
      "",
    );

    if (comment != newComment) {
      await this.setComment(newComment);
      return true;
    }
    return false;
  }
}
// #endregion Comment

// #region Migration
class CHMigration {
  private inner: CHBuilder.BuilderFactory;
  private comment: CHTableComment;
  public constructor(inner: CHBuilder.BuilderFactory, comment: CHTableComment) {
    this.inner = inner;
    this.comment = comment;
  }

  public log(msg: string) {
    this.inner.loggingFunction(
      `ClickhouseModel(${this.inner.tableName}) Migration: ${msg}`,
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
      `ALTER TABLE ${this.inner.tableName} DROP INDEX ${skipping.name}`,
    );
  }
  private async createSkipping(
    skipping: CHBuilder.Skipping,
  ): Promise<undefined> {
    await this.command(
      `ALTER TABLE ${this.inner.tableName} ADD INDEX ${skipping.name} ${
        skipping.expr
      } TYPE ${skipping.type.toString()}`,
    );
    await this.command(
      `ALTER TABLE ${this.inner.tableName} MATERIALIZE INDEX ${skipping.name}`,
    );
  }
  private async syncSkipping(): Promise<boolean> {
    // Get skippings from database
    let updated = false;
    const query = await getClient().query({
      query: `SELECT name, expr, type FROM system.data_skipping_indices WHERE table = {tableName:String}`,
      format: "JSONEachRow",
      query_params: {
        tableName: this.inner.tableName,
      },
    });
    const dbList = (await query.json()) as CHBuilder.Skipping[];

    // Compare from database's skippings
    for (const dbItem of dbList) {
      const matched = this.inner.skippingList.find(
        (i) => i.name == dbItem.name,
      );

      // Should be deleted
      if (!matched) {
        this.log(`update skipping '${dbItem.name}'`);
        await this.deleteSkipping(dbItem);
        updated = true;
        continue;
      }

      // Should be updated
      if (
        normalizeForDiff(matched.expr) != normalizeForDiff(dbItem.expr) ||
        normalizeForDiff(matched.type.toString()) !=
          normalizeForDiff(dbItem.type.toString())
      ) {
        this.log(`update skipping '${matched.name}'`);
        await this.deleteSkipping(matched);
        await this.createSkipping(matched);
        updated = true;
      }
    }

    // Should be created
    for (const item of this.inner.skippingList) {
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
      type: normalizeForDiff(base.type.toString()),
      default: normalizeForDiff(base.default) ?? "",
      codec: normalizeForDiff(base.codec)?.toUpperCase() ?? "",
    };
  }
  private static dbColumnNormalize(
    base: CHMigration.DBColumn,
  ): CHMigration.NormalizedColumn {
    return {
      name: base.name.trim(),
      type: normalizeForDiff(base.type),
      default: normalizeForDiff(base.default_expression) ?? "",
      codec: normalizeForDiff(base.compression_codec)?.toUpperCase() ?? "",
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

    // codec is different(empty not match)
    const dbCodecIsEmpty =
      db.codec == "" || db.codec.includes("NONE") || db.codec.includes("LZ4");
    const defineCodecIsEmpty = define.codec == "" || define.codec == "NONE";
    if (defineCodecIsEmpty != dbCodecIsEmpty) return true;

    // codec is different(content is not match)
    if (!defineCodecIsEmpty && !db.codec.includes(define.codec)) return true;

    return false;
  }
  private async syncColumn(allowDestructive: boolean): Promise<boolean> {
    let updated = false;
    const query = await getClient().query({
      query: `SELECT name, type, default_expression, compression_codec 
              FROM system.columns WHERE table = {tableName:String} AND database = currentDatabase()`,
      format: "JSONEachRow",
      query_params: {
        tableName: this.inner.tableName,
      },
    });
    const dbList = (await query.json()) as CHMigration.DBColumn[];

    for (const item of this.inner.columnList) {
      if (item.unmigratable) continue;
      const dbItem = dbList.find((i) => i.name === item.name);

      // Construct sql parts
      const defaultPart = item.default ? `DEFAULT ${item.default}` : "";
      const codecPart = item.codec ? `CODEC(${item.codec})` : "";
      const fullDefine =
        `${item.type.toString()} ${defaultPart} ${codecPart}`.trim();

      // Create new column
      if (!dbItem) {
        this.log(`create new column '${item.name}'`);
        await this.command(
          `ALTER TABLE ${this.inner.tableName} ADD COLUMN ${item.name} ${fullDefine}`,
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
          `ALTER TABLE ${this.inner.tableName} MODIFY COLUMN ${item.name} ${fullDefine}`,
        );
        updated = true;
      }
    }

    // Show column should be deleted
    for (const dbItem of dbList) {
      if (this.inner.columnList.find((i) => i.name == dbItem.name)) continue;
      const dropQuery = `ALTER TABLE ${this.inner.tableName} DROP COLUMN ${dbItem.name};`;
      if (allowDestructive) {
        await this.command(dropQuery);
        updated = true;
      } else {
        this.log(
          `column '${dbItem.name}' should be deleted! destructive migration must be done by user.\n QUERY: ${dropQuery}`,
        );
      }
    }

    return updated;
  }

  // Manage ttl
  private async syncTTL(): Promise<boolean> {
    const dbTTL = (await this.comment.getValue("#last_ttl")) ?? "";
    const TTL = this.inner.ttlString ?? "";

    if (dbTTL == TTL) return false;

    if (TTL == "") {
      this.log(`delete table TTL`);
      await this.command(`ALTER TABLE ${this.inner.tableName} REMOVE TTL`);
      await this.comment.deleteValue("#last_ttl");
    } else {
      this.log(`update table TTL 'TTL ${this.inner.ttlString}'`);
      await this.command(
        `ALTER TABLE ${this.inner.tableName} MODIFY TTL ${this.inner.ttlString};`,
      );
      await this.comment.setValue("#last_ttl", TTL);
    }

    return true;
  }

  // Create / manage table
  public async sync(allowDestructive: boolean): Promise<undefined> {
    try {
      await getClient().command({
        query: `
          CREATE TABLE IF NOT EXISTS ${this.inner.tableName} (
            timestamp DateTime64(3) DEFAULT now64(3),
            eventId UUID DEFAULT generateUUIDv4(),
            level Enum8('DEBUG'=1, 'INFO'=2, 'WARN'=3, 'ERROR'=4, 'FATAL'=5)
          )
          ENGINE = MergeTree()
          ORDER BY (timestamp, eventId)
          PARTITION BY toYYYYMM(timestamp)
        `,
      });
      let updated = false;
      if (await this.syncColumn(allowDestructive)) updated = true;
      if (await this.syncSkipping()) updated = true;
      if (await this.syncTTL()) updated = true;

      if (updated) {
        const sql = await getCreateTableSqlFor(this.inner.tableName);
        this.log(`table updated. SQL:\n${sql}`);
      }
    } catch (e) {
      const sql = await getCreateTableSqlFor(this.inner.tableName);
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
  }
  // DB 와의 비교를 위한 선언과 DB 컬럼의 중간 형태
  export interface NormalizedColumn {
    name: string;
    type: string;
    default: string;
    codec: string;
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
  public addParam(key: string, value: any): string {
    const define = this.columns.find((i) => i.name == key);
    if (!define) throw Error(`Column '${key}' not found from definition`);
    return this.addTypedParam(
      define.queryValueType ?? define.type.toString(),
      value,
    );
  }

  // 인자를 추가하고 그에 맞는 쿼리 스트링을 반환
  public addTypedParam(type: string, value: any): string {
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

        this.push(this.addParam(key, element));
      }
      this.push(")");
    } else if (operator == "BETWEEN") {
      this.push(this.addParam(key, value[0]))
        .push(" AND ")
        .push(this.addParam(key, value[1]));
    } else {
      this.push(this.addParam(key, value));
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
        this.push(this.addTypedParam(statement.type, statement.value));
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
  export type DataQuery<Fields extends CHBuilder.FieldsType> = {
    data: Pick<
      { [K in keyof Fields]: Fields[K]["query"] },
      keyof {
        [K in keyof Fields as Fields[K]["vaild"] extends true
          ? Fields[K]["hasDefault"] extends false
            ? K
            : never
          : never]?: true;
      }
    >;
  };

  // Result type
  export type Result<
    Fields extends CHBuilder.FieldsType,
    Query extends SelectQuery<Fields>,
  > = Query["select"] extends {}
    ? Pick<
        { [K in keyof Fields]: Fields[K]["selected"] },
        keyof CHQuery.OnlyTrue<Query["select"]>
      >
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
  private ttlString: string | undefined;
  private loggingFunction: (msg: string) => void;
  private autoSyncEnabled: boolean;
  private autoSyncAllowDestructive: boolean;
  private locked: boolean;
  private extendAllowed: boolean;

  public static fromFactory(factory: CHBuilder.BuilderFactory) {
    const builder = new CHBuilder(factory.tableName, factory.loggingFunction);
    builder.locked = true;
    builder.columnList = factory.columnList;
    builder.skippingList = factory.skippingList;
    builder.ttlString = factory.ttlString;
    builder.autoSyncEnabled = factory.autoSyncEnabled;
    builder.autoSyncAllowDestructive = factory.autoSyncAllowDestructive;
    return builder;
  }

  public constructor(
    tableName: string,
    loggingFunction: CHBuilder["loggingFunction"] = console.log.bind(console),
  ) {
    if (!verifyName(tableName)) {
      throw Error(`Table name '${tableName}' is not vaild`);
    }

    this.locked = false;
    this.extendAllowed = true;
    this.tableName = tableName;
    this.skippingList = [];
    this.columnList = [...CHBuilder.DefaultColumns];
    this.loggingFunction = loggingFunction;
    this.autoSyncEnabled = false;
    this.autoSyncAllowDestructive = false;
  }

  /**
   * 빌더를 읽기 전용 모드로 변경합니다. 모델간 공통된 필드를 구현하기 위해
   * withExtend 과 같이 사용될 수 있습니다.
   */
  public lock(): this {
    this.locked = true;
    return this;
  }

  /**
   * 빌더로 부터 모델을 생성합니다. 생성 이후 빌더는 읽기 전용 모드로 변경됩니다.
   */
  public build(): CHModel<Fields> {
    if (this.locked) {
      throw Error("Create model from readonly builder is not allowed");
    }

    this.locked = true;
    return new CHModel({
      skippingList: this.skippingList,
      columnList: this.columnList,
      tableName: this.tableName,
      ttlString: this.ttlString,
      loggingFunction: this.loggingFunction,
      autoSyncEnabled: this.autoSyncEnabled,
      autoSyncAllowDestructive: this.autoSyncAllowDestructive,
    });
  }

  /**
   * 빌더가 읽기 전용 상태이면 오류를 발생시킵니다.
   */
  public ensureWritable(): this {
    if (this.locked) {
      throw Error(
        "This builder instance is in a read-only state. Write operations cannot be performed.",
      );
    }
    return this;
  }

  /**
   * 모델이 빌드 될 때 백그라운드에서 자동으로 마이그레이션을 시도합니다.
   */
  public withAutoSync(allowDestructive: boolean = false): this {
    this.extendAllowed = false;
    this.autoSyncEnabled = true;
    this.autoSyncAllowDestructive = allowDestructive;
    return this;
  }

  /**
   * 런타임 로깅 함수를 지정합니다
   */
  public withLoggingFunction(func: CHBuilder["loggingFunction"]): this {
    this.extendAllowed = false;
    this.ensureWritable();
    this.loggingFunction = func;
    return this;
  }

  /**
   * 테이블의 TTL 을 조절합니다. 예: `date + INTERVAL 1 MONTH`
   */
  public withTTL(ttl: string): this {
    this.extendAllowed = false;
    this.ensureWritable();
    this.ttlString = ttl;
    return this;
  }

  /**
   * 기존의 Builder의 구성 요소를 가져옵니다.
   */
  public withExtend<
    TargetFields extends CHBuilder.FieldsType = CHBuilder.DefaultFields,
  >(target: CHBuilder<TargetFields>): CHBuilder<Fields & TargetFields> {
    this.ensureWritable();
    if (!this.extendAllowed) {
      throw Error(
        "`withExtend` is not allowed. `withExtend` must be executed before other `with` methods.",
      );
    }
    this.columnList = [...this.columnList, ...target.columnList];
    this.skippingList = [...this.skippingList, ...target.skippingList];
    this.ttlString = target.ttlString;
    this.loggingFunction = target.loggingFunction;
    this.autoSyncEnabled = target.autoSyncEnabled;
    this.autoSyncAllowDestructive = target.autoSyncAllowDestructive;
    return this as any;
  }

  /**
   * 사용자 지정 컬럼을 추가합니다
   */
  public withColumn<
    Name extends string,
    Type extends keyof CHBuilder.ClickHouseTypeMap | CustomColumnType,
    Default extends string | undefined = undefined,
    Schema extends TSchema | undefined = undefined,
    QueryValueSchema extends TSchema | undefined = undefined,
  >(
    column: CHBuilder.Column<Name, Type, Default, Schema, QueryValueSchema>,
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
          : Schema extends TSchema
          ? Static<Schema>
          : Type extends keyof CHBuilder.ClickHouseTypeMap
          ? CHBuilder.ClickHouseTypeMap[Type]
          : never;
        hasDefault: Default extends string ? true : false;
        vaild: true;
      };
    }
  > {
    this.extendAllowed = false;
    this.ensureWritable();
    if (!verifyName(column.name)) {
      throw Error(`Column name '${column.name}' is not vaild`);
    }
    this.columnList.push(column as CHBuilder.Column);
    return this as any;
  }

  /**
   * level 컬럼을 추가합니다. 로깅 데이터를 위해 일반적으로 사용되는
   * `"DEBUG" | "INFO" | "WARN" | "ERROR" | "FATAL"` enum 을 사용합니다.
   * level 컬럼에 대해 자동으로 인덱스를 생성합니다
   */
  public withLevel(): CHBuilder<
    Fields & {
      level: {
        selected: CHBuilder.LogLevel;
        query: CHBuilder.LogLevel;
        hasDefault: false;
        vaild: true;
      };
    }
  > {
    this.extendAllowed = false;
    this.ensureWritable();
    this.columnList.push({
      name: "level",
      type: new CustomColumnType(
        "Enum8('DEBUG'=1, 'INFO'=2, 'WARN'=3, 'ERROR'=4, 'FATAL'=5)",
      ),
      schema: Type.Union([
        Type.Literal("DEBUG"),
        Type.Literal("INFO"),
        Type.Literal("WARN"),
        Type.Literal("ERROR"),
        Type.Literal("FATAL"),
      ]),
      queryValueType: "String",
    });
    this.withSkipping({
      expr: "level",
      name: "level_index",
      type: "set",
    });
    return this as any;
  }

  /**
   * 스킵 인덱스를 추가합니다
   */
  public withSkipping(skipping: CHBuilder.Skipping): this {
    this.extendAllowed = false;
    this.ensureWritable();
    if (!verifyName(skipping.name)) {
      throw Error(`Skipping index name '${skipping.name}' is not vaild`);
    }
    this.skippingList.push(skipping);
    return this;
  }
}
export namespace CHBuilder {
  export interface BuilderFactory {
    skippingList: CHBuilder.Skipping[];
    columnList: CHBuilder.Column[];
    tableName: string;
    ttlString: string | undefined;
    loggingFunction: (msg: string) => void;
    autoSyncEnabled: boolean;
    autoSyncAllowDestructive: boolean;
  }

  // Field is type holder for ClickhouseModel
  export type FieldsType = {
    [key: string]: {
      selected: any;
      query: any;
      hasDefault: boolean;
      vaild: false;
    };
  };

  // Define default columns and default fields
  export const DefaultColumns = [
    {
      name: "timestamp",
      type: "DateTime64(3)",
      default: "now64(3)",
      unmigratable: true,
    },
    {
      name: "eventId",
      type: "UUID",
      default: "generateUUIDv4()",
      unmigratable: true,
    },
  ] as const;
  export type LogLevel = "DEBUG" | "INFO" | "WARN" | "ERROR" | "FATAL";
  export type DefaultFields = {
    timestamp: {
      selected: string;
      query: string;
      hasDefault: true;
      vaild: true;
    };
    eventId: { selected: string; query: string; hasDefault: true; vaild: true };
  } & FieldsType;

  // 스킵 색인 필드 선언
  export interface Skipping {
    name: string;
    expr: string;
    type: SkippingType;
  }
  export type SkippingType =
    | "minmax"
    | "set"
    | "bloom_filter"
    | "ngrambf_v1"
    | "tokenbf_v1"
    | "text"
    | CustomSkippingType;

  // 컬럼 선언
  export interface Column<
    Name extends string = string,
    Type extends keyof ClickHouseTypeMap | CustomColumnType =
      | keyof ClickHouseTypeMap
      | CustomColumnType,
    Default extends string | undefined = string | undefined,
    Schema extends TSchema | undefined = TSchema | undefined,
    QueryValueSchema extends TSchema | undefined = TSchema | undefined,
  > {
    name: Name;
    type: Type;
    default?: Default; // 예: "generateUUIDv4()"
    codec?: string; // 예: "ZSTD(3)"
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
     * 쿼리 시 @clickhouse/client 클라이언트에 전달하는 값의 타입입니다. 만약 설정되지 않으면
     * schema 를 사용하고 schema 도 없으면 기본 타입을 사용합니다.
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
  private inner: CHBuilder.BuilderFactory;
  private comment: CHTableComment;

  constructor(inner: CHBuilder.BuilderFactory) {
    this.inner = inner;
    this.comment = new CHTableComment(inner.tableName);

    if (inner.autoSyncEnabled) {
      this.sync(inner.autoSyncAllowDestructive);
    }
  }

  public log(msg: string) {
    this.inner.loggingFunction(
      `ClickhouseModel(${this.inner.tableName}) ${msg}`,
    );
  }

  /** 이 모델에 대한 빌더를 반환합니다.
   * 잠긴 상태로 반환되며 withExtend 이외의 목적으로 사용되지 않아야합니다. */
  public get builder(): CHBuilder<Fields> {
    return CHBuilder.fromFactory(this.inner) as any;
  }

  /**
   * 데이터베이스의 스키마를 업데이트합니다.
   */
  public async sync(allowDestructive: boolean = false): Promise<this> {
    const migrationHandle = new CHMigration(this.inner, this.comment);
    await migrationHandle.sync(allowDestructive);
    return this;
  }

  /**
   * 데이터를 검색합니다. 배열로 검색된 요소를 반환합니다.
   */
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

      logQuery?: boolean;
      dryRun?: boolean;
    },
    Result = CHQuery.Result<Fields, Query>,
  >(query: Query): Promise<Result[]> {
    const builder = new CHQuery(this.inner.columnList)
      .pushSelectClause(query)
      .pushFromClause(this.inner.tableName)
      .pushWhereClause(query)
      .pushOrderByClause(query)
      .pushLimitClause(query);

    const queryString = builder.renderQuery();
    const paramList = builder.mapParamList();

    if (query.logQuery) {
      const data = util.inspect(paramList, { depth: 5, maxArrayLength: 10 });
      this.log(`Query:\n${queryString}Data: ${data}`);
    }

    if (query.dryRun) {
      return [];
    }

    const resultSet = await getClient().query({
      query: queryString,
      query_params: paramList,
      format: "JSONEachRow",
    });

    return (await resultSet.json<Result>()) ?? [];
  }

  /**
   * 데이터를 검색합니다. 요소를 찾지 못한 경우 null 을 반환합니다
   */
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

      logQuery?: boolean;
      dryRun?: boolean;
    },
    Result = CHQuery.Result<Fields, Query>,
  >(query: Query): Promise<Result | null> {
    const builder = new CHQuery(this.inner.columnList)
      .pushSelectClause(query)
      .pushFromClause(this.inner.tableName)
      .pushWhereClause(query)
      .pushOrderByClause(query)
      .pushLimitClause({ limit: 1 });

    const queryString = builder.renderQuery();
    const paramList = builder.mapParamList();

    if (query.logQuery) {
      const data = util.inspect(paramList, { depth: 5, maxArrayLength: 10 });
      this.log(`Query:\n${queryString}Data: ${data}`);
    }

    if (query.dryRun) {
      return null;
    }

    const resultSet = await getClient().query({
      query: queryString,
      query_params: paramList,
      format: "JSONEachRow",
    });

    return (await resultSet.json<Result>())?.[0] ?? null;
  }

  /**
   * 데이터를 추가합니다
   */
  public async insert(data: CHQuery.DataQuery<Fields>["data"]) {
    await getClient().insert({
      table: this.inner.tableName,
      values: data,
      format: "JSONEachRow",
    });
  }

  /**
   * 테이블의 선언에 데이터를 저장합니다. 이 동작은 매우 느리므로 자주 수행되어선 안됩니다.
   * 어플리케이션 수준에서 사용할 테이블에 특별히 저장할 옵션이나 값이 있는 경우 사용하세요.
   */
  public async setTableValue(key: string, value: string) {
    if (!verifyName(key)) {
      throw Error(`Table value key name '${key}' is not vaild`);
    }
    this.comment.setValue(key, value);
  }
  /**
   * 테이블의 선언에 저장된 데이터를 읽습니다. 이 동작은 캐싱이 수행되며 항상 최신의 값을 가지지 않을 수 있습니다.
   * 어플리케이션 수준에서 사용할 테이블에 특별히 저장할 옵션이나 값이 있는 경우 사용하세요.
   */
  public async getTableValue(key: string): Promise<string | undefined> {
    if (!verifyName(key)) {
      throw Error(`Table value key name '${key}' is not vaild`);
    }
    return await this.comment.getValue(key);
  }
  /**
   * 테이블의 선언에 저장된 데이터를 제거합니다. 이 동작은 매우 느리므로 자주 수행되어선 안됩니다.
   * 제거된 경우 true 를, 존재하지 않아 제거하지 못한 경우 false 을 반환합니다
   * 어플리케이션 수준에서 사용할 테이블에 특별히 저장할 옵션이나 값이 있는 경우 사용하세요.
   */
  public async deleteTableValue(key: string): Promise<boolean> {
    if (!verifyName(key)) {
      throw Error(`Table value key name '${key}' is not vaild`);
    }
    return await this.comment.deleteValue(key);
  }
}
// #endregion Model
