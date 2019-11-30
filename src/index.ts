import { CancellationToken, Financial, Logger } from "@zxteam/contract";
import { Disposable, Initable } from "@zxteam/disposable";
import { ArgumentError, CancelledError, InvalidOperationError } from "@zxteam/errors";
import { financial } from "@zxteam/financial";
import { SqlProviderFactory, SqlProvider, SqlStatement, SqlStatementParam, SqlResultRecord, SqlData, SqlTemporaryTable, SqlDialect } from "@zxteam/sql";

import * as _ from "lodash";
import * as pg from "pg";

const DATA_TYPE_ID_EMPTY = 2278; // Return postgres if data is null
const DATA_TYPE_ID_MULTI = 1790; // Return postgres if data is multy

export class PostgresProviderFactory extends Initable implements SqlProviderFactory {
	private readonly _log: Logger;
	private readonly _url: URL;
	private readonly _pool: pg.Pool;

	// This implemenation wrap package https://www.npmjs.com/package/pg
	public constructor(opts: PostgresProviderFactory.Opts) {
		super();
		this._url = opts.url;
		this._log = opts.log !== undefined ? opts.log : DUMMY_LOGGER;
		this._log.trace("PostgresProviderPoolFactory constructed");

		const poolConfig: pg.PoolConfig = { host: this._url.hostname };

		if (!_.isEmpty(this._url.port)) { poolConfig.port = Number.parseInt(this._url.port); }
		if (!_.isEmpty(this._url.username)) { poolConfig.user = this._url.username; }
		if (!_.isEmpty(this._url.password)) { poolConfig.password = this._url.password; }

		// DB name
		let pathname = this._url.pathname;
		while (pathname.length > 0 && pathname[0] === "/") { pathname = pathname.substring(1); }
		poolConfig.database = pathname;

		// Timeouts
		if (opts.connectionTimeoutMillis !== undefined) { poolConfig.connectionTimeoutMillis = opts.connectionTimeoutMillis; }
		if (opts.connectionTimeoutMillis !== undefined) { poolConfig.idleTimeoutMillis = opts.idleTimeoutMillis; }

		// App name
		if (!_.isEmpty(opts.applicationName)) { poolConfig.application_name = opts.applicationName; }

		// SSL
		if (opts.ssl !== undefined) {
			poolConfig.ssl = {};
			if (opts.ssl.caCert !== undefined) {
				poolConfig.ssl.ca = opts.ssl.caCert;
			}
			if (opts.ssl.clientCert !== undefined) {
				poolConfig.ssl.cert = opts.ssl.clientCert.cert;
				poolConfig.ssl.key = opts.ssl.clientCert.key;
			}
		}

		this._pool = new pg.Pool(poolConfig);
	}

	public async create(cancellationToken: CancellationToken): Promise<SqlProvider> {
		this._log.trace("Creating Postgres SqlProvider...");


		this._log.trace("Accuring Postgres client...");
		const pgClient = await this._pool.connect();
		try {
			this._log.trace("Check cancellationToken for interrupt");
			cancellationToken.throwIfCancellationRequested();

			this._log.trace("Creating instance of a PostgresSqlProvider...");
			const sqlProvider: SqlProvider = new PostgresSqlProvider(
				pgClient,
				async () => {
					// dispose callback
					pgClient.release();
				},
				this._log
			);

			return sqlProvider;
		} catch (e) {
			pgClient.release();
			throw e;
		}
	}

	protected onInit(cancellationToken: CancellationToken): void | Promise<void> {
		//
	}
	protected async onDispose(): Promise<void> {
		// Dispose never raise error
		try {
			await this._pool.end();
		} catch (e) {
			if (this._log.isWarnEnabled) {
				this._log.warn("Module 'pg' ends pool with error", e);
			} else {
				console.error("Module 'pg' ends pool with error", e);
			}
		}
	}
}

export namespace PostgresProviderFactory {
	export interface Opts {
		readonly url: URL;
		readonly applicationName?: string;
		readonly log?: Logger;
		readonly connectionTimeoutMillis?: number;
		readonly idleTimeoutMillis?: number;
		readonly ssl?: {
			readonly caCert?: Buffer;
			readonly clientCert?: {
				readonly cert: Buffer;
				readonly key: Buffer;
			}
		};
	}
}

export default PostgresProviderFactory;

class PostgresSqlProvider extends Disposable implements SqlProvider {
	public readonly dialect: SqlDialect = SqlDialect.PostgreSQL;
	public readonly pgClient: pg.PoolClient;
	public readonly log: Logger;
	private readonly _disposer: () => Promise<void>;
	public constructor(pgClient: pg.PoolClient, disposer: () => Promise<void>, log: Logger) {
		super();
		this.pgClient = pgClient;
		this._disposer = disposer;
		this.log = log;
		this.log.trace("PostgresSqlProvider constructed");
	}

	public statement(sql: string): PostgresSqlStatement {
		super.verifyNotDisposed();
		if (!sql) { throw new ArgumentError("sql"); }
		this.log.trace("Statement: ", sql);
		return new PostgresSqlStatement(this, sql);
	}

	public async createTempTable(
		cancellationToken: CancellationToken, tableName: string, columnsDefinitions: string
	): Promise<SqlTemporaryTable> {
		const tempTable = new PostgresTempTable(this, cancellationToken, tableName, columnsDefinitions);
		await tempTable.init(cancellationToken);
		return tempTable;
	}

	protected async onDispose(): Promise<void> {
		this.log.trace("Disposing");
		await this._disposer();
		this.log.trace("Disposed");
	}
}

class PostgresSqlStatement implements SqlStatement {
	private readonly _sqlText: string;
	private readonly _owner: PostgresSqlProvider;

	public constructor(owner: PostgresSqlProvider, sql: string) {
		this._owner = owner;
		this._sqlText = sql;
		this._owner.log.trace("PostgresSqlStatement constructed");
	}

	public async execute(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Promise<void> {
		await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(values)
		);
	}

	public async executeSingle(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Promise<SqlResultRecord> {
		const underlyingResult = await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(values)
		);

		const underlyingResultRows = underlyingResult.rows;
		const underlyingResultFields = underlyingResult.fields;

		if (underlyingResultFields[0].dataTypeID === DATA_TYPE_ID_MULTI) {
			throw new InvalidOperationError("executeQuery does not support multi request");
		}

		if (underlyingResultRows.length === 1 && !(underlyingResultFields[0].dataTypeID === DATA_TYPE_ID_EMPTY)) {
			return new PostgresSqlResultRecord(underlyingResultRows[0], underlyingResultFields);
		} else {
			throw new InvalidOperationError("SQL query returns non-single result");
		}
	}

	public async executeQuery(
		cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>
	): Promise<Array<SqlResultRecord>> {
		const underlyingResult = await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(values)
		);

		const underlyingResultRows = underlyingResult.rows;
		const underlyingResultFields = underlyingResult.fields;

		if (underlyingResultFields[0].dataTypeID === DATA_TYPE_ID_MULTI) {
			throw new InvalidOperationError("executeQuery does not support multiset request yet");
		}

		if (underlyingResultRows.length > 0 && !(underlyingResultFields[0].dataTypeID === DATA_TYPE_ID_EMPTY)) {
			return underlyingResultRows.map(row => new PostgresSqlResultRecord(row, underlyingResultFields));
		} else {
			return [];
		}
	}

	// tslint:disable-next-line:max-line-length
	public async executeQueryMultiSets(
		cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>
	): Promise<Array<Array<SqlResultRecord>>> {
		// Executing: BEGIN
		await helpers.executeRunQuery(cancellationToken, this._owner.pgClient, "BEGIN TRANSACTION", []);
		try {
			cancellationToken.throwIfCancellationRequested();

			const resultFetchs = await helpers.executeRunQuery(
				cancellationToken,
				this._owner.pgClient,
				this._sqlText,
				helpers.statementArgumentsAdapter(values)
			);
			cancellationToken.throwIfCancellationRequested();

			// Verify that this is a multi-request
			if (resultFetchs.fields[0].dataTypeID !== DATA_TYPE_ID_MULTI) {
				// This is not a multi request. Raise exception.
				throw new InvalidOperationError(`executeQueryMultiSets cannot execute this script: ${this._sqlText}`);
			}

			const resultFetchsValue = helpers.parsingValue(resultFetchs);
			const friendlyResult: Array<Array<SqlResultRecord>> = [];
			for (let i = 0; i < resultFetchsValue.length; i++) {
				const fetch = resultFetchsValue[i];

				const queryFetchs = await helpers.executeRunQuery(cancellationToken, this._owner.pgClient, `FETCH ALL IN "${fetch}";`, []);
				cancellationToken.throwIfCancellationRequested();

				friendlyResult.push(queryFetchs.rows.map(row => new PostgresSqlResultRecord(row, queryFetchs.fields)));
			}

			// Executing: COMMIT
			await helpers.executeRunQuery(cancellationToken, this._owner.pgClient, "COMMIT", []);

			return friendlyResult;
		} catch (e) {
			// Executing: ROLLBACK
			await helpers.executeRunQuery(cancellationToken, this._owner.pgClient, "ROLLBACK", []);
			throw e;
		}
	}

	public async executeScalar(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Promise<SqlData> {
		const underlyingResult = await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(values)
		);

		const underlyingRows = underlyingResult.rows;
		const underlyingFields = underlyingResult.fields;
		if (underlyingRows.length > 0) {
			if (underlyingFields[0].dataTypeID === DATA_TYPE_ID_MULTI) {
				throw new InvalidOperationError("executeScalar does not support multiset request yet");
			}

			const underlyingFirstRow = underlyingRows[0];
			const value = underlyingFirstRow[Object.keys(underlyingFirstRow)[0]];
			const fi = underlyingFields[0];
			if (value !== undefined || fi !== undefined) {
				return new PostgresData(value, fi);
			} else {
				throw new ArgumentError("values", `Bad argument ${value} and ${fi}`);
			}
		} else {
			throw new Error("Underlying Postgres provider returns not enough data to complete request.");
		}
	}

	public async executeScalarOrNull(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Promise<SqlData | null> {
		const underlyingResult = await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(values)
		);

		const underlyingRows = underlyingResult.rows;
		const underlyingFields = underlyingResult.fields;
		if (underlyingRows.length > 0) {
			if (underlyingFields[0].dataTypeID === DATA_TYPE_ID_MULTI) {
				throw new InvalidOperationError("executeScalarOrNull does not support multiset request yet");
			}

			const underlyingFirstRow = underlyingRows[0];
			const value = underlyingFirstRow[Object.keys(underlyingFirstRow)[0]];
			const fi = underlyingFields[0];
			if (value !== undefined || fi !== undefined) {
				return new PostgresData(value, fi);
			} else {
				throw new Error(`Bad result ${value} and ${fi}`);
			}
		} else {
			return null;
		}
	}
}

namespace PostgresSqlResultRecord {
	export type NameMap = {
		[name: string]: pg.FieldDef;
	};
}
class PostgresSqlResultRecord implements SqlResultRecord {
	private readonly _fieldsData: any;
	private readonly _fieldsInfo: Array<pg.FieldDef>;
	private _nameMap?: PostgresSqlResultRecord.NameMap;

	public constructor(fieldsData: any, fieldsInfo: Array<pg.FieldDef>) {
		if (Object.keys(fieldsData).length !== fieldsInfo.length) {
			throw new Error("Internal error. Fields count is not equal to data columns.");
		}
		this._fieldsData = fieldsData;
		this._fieldsInfo = fieldsInfo;
	}

	public get(name: string): SqlData;
	public get(index: number): SqlData;
	public get(nameOrIndex: string | number): SqlData {
		if (typeof nameOrIndex === "string") {
			return this.getByName(nameOrIndex);
		} else {
			return this.getByIndex(nameOrIndex);
		}
	}

	private get nameMap(): PostgresSqlResultRecord.NameMap {
		if (this._nameMap === undefined) {
			const nameMap: PostgresSqlResultRecord.NameMap = {};
			const total = this._fieldsInfo.length;
			for (let index = 0; index < total; ++index) {
				const fi: pg.FieldDef = this._fieldsInfo[index];
				if (fi.name in nameMap) { throw new Error("Cannot access SqlResultRecord by name due result set has name duplicates"); }
				nameMap[fi.name] = fi;
			}
			this._nameMap = nameMap;
		}
		return this._nameMap;
	}

	private getByIndex(index: number): SqlData {
		const fi: pg.FieldDef = this._fieldsInfo[index];
		const value: any = this._fieldsData[fi.name];
		return new PostgresData(value, fi);
	}
	private getByName(name: string): SqlData {
		const fi = this.nameMap[name];
		const value: any = this._fieldsData[fi.name];
		return new PostgresData(value, fi);
	}
}

class PostgresTempTable extends Initable implements SqlTemporaryTable {

	private readonly _owner: PostgresSqlProvider;
	private readonly _cancellationToken: CancellationToken;
	private readonly _tableName: string;
	private readonly _columnsDefinitions: string;

	public constructor(owner: PostgresSqlProvider, cancellationToken: CancellationToken, tableName: string, columnsDefinitions: string) {
		super();
		this._owner = owner;
		this._cancellationToken = cancellationToken;
		this._tableName = tableName;
		this._columnsDefinitions = columnsDefinitions;
	}

	public bulkInsert(cancellationToken: CancellationToken, bulkValues: Array<Array<SqlStatementParam>>): Promise<void> {
		return this._owner.statement(`INSERT INTO \`${this._tableName}\``).execute(cancellationToken, bulkValues as any);
	}
	public clear(cancellationToken: CancellationToken): Promise<void> {
		return this._owner.statement(`DELETE FROM \`${this._tableName}\``).execute(cancellationToken);
	}
	public insert(cancellationToken: CancellationToken, values: Array<SqlStatementParam>): Promise<void> {
		return this._owner.statement(`INSERT INTO \`${this._tableName}\``).execute(cancellationToken, ...values);
	}

	protected async onInit(): Promise<void> {
		await this._owner.statement(`CREATE TEMPORARY TABLE ${this._tableName} (${this._columnsDefinitions})`).execute(this._cancellationToken);
	}
	protected async onDispose(): Promise<void> {
		try {
			await this._owner.statement(`DROP TABLE ${this._tableName}`).execute(this._cancellationToken);
		} catch (e) {
			// dispose never raise error
			if (e instanceof CancelledError) {
				return; // skip error message if task was cancelled
			}
			// Dispose never raise errors
			console.error(e); // we cannot do anymore here, just log
		}
	}
}

class PostgresData implements SqlData {
	private readonly _postgresValue: any;
	private readonly _fi: pg.FieldDef;

	public get asBoolean(): boolean {
		if (typeof this._postgresValue === "boolean") {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableBoolean(): boolean | null {
		if (this._postgresValue === null) {
			return null;
		} else if (typeof this._postgresValue === "boolean") {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asString(): string {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._postgresValue === "string") {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableString(): string | null {
		if (this._postgresValue === null) {
			return null;
		} else if (typeof this._postgresValue === "string") {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asInteger(): number {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._postgresValue === "number" && Number.isInteger(this._postgresValue)) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableInteger(): number | null {
		if (this._postgresValue === null) {
			return null;
		} else if (typeof this._postgresValue === "number" && Number.isInteger(this._postgresValue)) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNumber(): number {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._postgresValue === "number") {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableNumber(): number | null {
		if (this._postgresValue === null) {
			return null;
		} else if (typeof this._postgresValue === "number") {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asFinancial(): Financial {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._postgresValue === "number") {
			return financial.fromFloat(this._postgresValue);
		} else if (typeof this._postgresValue === "string") {
			return financial.parse(this._postgresValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableFinancial(): Financial | null {
		if (this._postgresValue === null) {
			return null;
		} else if (typeof this._postgresValue === "number") {
			return financial.fromFloat(this._postgresValue);
		} else if (typeof this._postgresValue === "string") {
			return financial.parse(this._postgresValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asDate(): Date {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (this._postgresValue instanceof Date) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableDate(): Date | null {
		if (this._postgresValue === null) {
			return null;
		} else if (this._postgresValue instanceof Date) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asBinary(): Uint8Array {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (this._postgresValue instanceof Uint8Array) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableBinary(): Uint8Array | null {
		if (this._postgresValue === null) {
			return null;
		} else if (this._postgresValue instanceof Uint8Array) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}

	public constructor(postgresValue: any, fi: pg.FieldDef) {
		if (postgresValue === undefined) {
			throw new ArgumentError("postgresValue");
		}
		this._postgresValue = postgresValue;
		this._fi = fi;
	}

	private formatWrongDataTypeMessage(): string {
		return `Invalid conversion: requested wrong data type of field '${this._fi.name}'`;
	}
}

const DUMMY_LOGGER: Logger = Object.freeze({
	get isTraceEnabled(): boolean { return false; },
	get isDebugEnabled(): boolean { return false; },
	get isInfoEnabled(): boolean { return false; },
	get isWarnEnabled(): boolean { return false; },
	get isErrorEnabled(): boolean { return false; },
	get isFatalEnabled(): boolean { return false; },

	trace(message: string, ...args: any[]): void { /* NOP */ },
	debug(message: string, ...args: any[]): void { /* NOP */ },
	info(message: string, ...args: any[]): void { /* NOP */ },
	warn(message: string, ...args: any[]): void { /* NOP */ },
	error(message: string, ...args: any[]): void { /* NOP */ },
	fatal(message: string, ...args: any[]): void { /* NOP */ },

	getLogger(name?: string): Logger { /* NOP */ return this; }
});

namespace helpers {
	export function openDatabase(url: URL): Promise<pg.Client> {
		return new Promise((resolve, reject) => {
			const client: pg.Client = new pg.Client({
				host: url.hostname,
				port: url.port !== undefined ? Number.parseInt(url.port) : 5432,
				user: url.username,
				password: url.password,
				database: url.pathname.substr(1) // skip first symbol '/'
			});
			client.connect(err => {
				if (err) {
					return reject(err);
				}
				return resolve(client);
			});
		});
	}
	export function closeDatabase(db: pg.Client): Promise<void> {
		return new Promise((resolve, reject) => {
			db.end((error) => {
				if (error) { return reject(error); }
				return resolve();
			});
		});
	}
	export function executeRunQuery(
		cancellationToken: CancellationToken, db: pg.PoolClient, sqlText: string, values: Array<SqlStatementParam>
	): Promise<pg.QueryResult> {
		return new Promise<pg.QueryResult>((resolve, reject) => {
			db.query(sqlText, values,
				(err: any, underlyingResult: pg.QueryResult) => {
					if (err) {
						return reject(err);
					}
					return resolve(underlyingResult);
				});
		});
	}
	export function statementArgumentsAdapter(args: Array<SqlStatementParam>): Array<any> {
		return args.map(value => {
			if (typeof value === "object") {
				if (value !== null && financial.isFinancial(value)) {
					return value.toString(); // Financial should be converted to string
				}
			}
			return value;
		});
	}
	export function parsingValue(res: pg.QueryResult): Array<any> {
		const rows = res.rows;
		return rows.map((row) => row[Object.keys(row)[0]]);
	}

}
