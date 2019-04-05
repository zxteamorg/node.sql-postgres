import { Factory, Logger, CancellationToken, Task as TaskLike, Financial as FinancialLike } from "@zxteam/contract";
import {
	SqlProviderFactory, SqlProvider, SqlStatement, SqlStatementParam, SqlResultRecord, SqlData, SqlTemporaryTable
} from "@zxteam/contract.sql";
import { Disposable, Initable } from "@zxteam/disposable";
import { financial } from "@zxteam/financial.js";
import { Task, CancelledError } from "ptask.js";
import { URL } from "url";
import * as pg from "pg";


const FINACIAL_NUMBER_DEFAULT_FRACTION = 12;
const DATA_TYPE_ID_EMPTY = 2278; // Return postgres if data is null
const DATA_TYPE_ID_MULTI = 1790; // Return postgres if data is multy

function executeRunQuery(db: pg.PoolClient, sqlText: string, values: Array<SqlStatementParam>): Promise<pg.QueryResult> {
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

export class PostgresProviderFactory implements SqlProviderFactory {
	private readonly _logger: Logger;
	private readonly _url: URL;

	private _providesCount: number;
	private _postgresConnectionPool: pg.Pool | null;

	// This implemenation wrap package https://www.npmjs.com/package/pg
	public constructor(url: URL, logger?: Logger) {
		this._postgresConnectionPool = null;
		this._providesCount = 0;
		this._logger = logger || new DummyLogger();
		this._url = url;

		this._logger.trace("PostgresProviderFactory Constructed");
	}

	public create(cancellationToken?: CancellationToken): Task<SqlProvider> {
		const disposer = (connection: pg.PoolClient): Promise<void> => {
			connection.release();
			--this._providesCount;
			if (this._providesCount === 0) {
				return new Promise((closedResolve) => {
					if (this._postgresConnectionPool === null) {
						console.error("VERY UNEXPECTED ERROR! This should never happen! postgresConnectionPool === null while last provider is disposing");
						return closedResolve();
					}
					const mysqlConnectionPool = this._postgresConnectionPool;
					this._postgresConnectionPool = null;
					try {
						mysqlConnectionPool.end();
						return closedResolve();
					} catch (e) {
						throw new Error("Can't close postgres pool");
					}
				});
			}
			return Promise.resolve();
		};
		return Task.run((ct) => new Promise<SqlProvider>((resolve, reject) => {
			this._logger.trace("Creating Postgres SqlProvider..");

			if (ct.isCancellationRequested) { return reject(new CancelledError()); }

			if (this._postgresConnectionPool === null) {
				this._postgresConnectionPool = new pg.Pool({
					host: this._url.hostname,
					port: this._url.port !== undefined ? Number.parseInt(this._url.port) : 5432,
					user: this._url.username,
					password: this._url.password,
					database: this._url.pathname.substr(1) // skip first symbol '/'
				});
			}

			const mysqlConnectionPool = this._postgresConnectionPool;
			return mysqlConnectionPool.connect((err, connection) => {
				if (err) {
					this._logger.trace("Failed to create Postgres SqlProvider", err);
					if (this._providesCount === 0) {
						return mysqlConnectionPool.end();
					} else {
						return reject(err);
					}
				}

				if (ct.isCancellationRequested) { return reject(new CancelledError()); }

				try {
					this._logger.trace("Created Postgres SqlProvider");

					const sqlProvider: SqlProvider = new PostgresProvider(connection, () => disposer(connection), this._logger);
					++this._providesCount;
					this._logger.trace("Created MySQL SqlProvider");
					return resolve(sqlProvider);
				} catch (e) {
					connection.release();
					this._logger.trace("Failed to create MySQL SqlProvider", e);
					return reject(e);
				}
			});
		}), cancellationToken);
	}
}

export default PostgresProviderFactory;

class ArgumentError extends Error { }
class InvalidOperationError extends Error { }

class PostgresProvider extends Disposable implements SqlProvider {
	public readonly postgresConnection: pg.PoolClient;
	private readonly _logger: Logger;
	private readonly _disposer: () => Promise<void>;
	public constructor(postgresConnection: pg.PoolClient, disposer: () => Promise<void>, logger: Logger) {
		super();
		this.postgresConnection = postgresConnection;
		this._disposer = disposer;
		this._logger = logger;
		this._logger.trace("PostgresProvider Constructed");
	}

	public statement(sql: string): PostgresStatement {
		super.verifyNotDisposed();
		if (!sql) { throw new Error("sql"); }
		this._logger.trace("Statement: ", sql);
		return new PostgresStatement(this, sql, this._logger);
	}

	// tslint:disable-next-line:max-line-length
	public createTempTable(cancellationToken: CancellationToken, tableName: string, columnsDefinitions: string): TaskLike<SqlTemporaryTable> {
		return Task.run(async (ct) => {
			const tempTable = new PostgresTempTable(this, ct, tableName, columnsDefinitions);
			await tempTable.init();
			return tempTable;
		}, cancellationToken || undefined);
	}

	protected async onDispose(): Promise<void> {
		this._logger.trace("Disposing");
		await this._disposer();
		this._logger.trace("Disposed");
	}
}

class PostgresStatement implements SqlStatement {
	private readonly _logger: Logger;
	private readonly _sqlText: string;
	private readonly _owner: PostgresProvider;

	public constructor(owner: PostgresProvider, sql: string, logger: Logger) {
		this._owner = owner;
		this._sqlText = sql;
		this._logger = logger;
		if (this._logger.isTraceEnabled) { this._logger.trace("PostgresStatement Constructed"); }
	}

	public execute(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<void> {
		return Task.run((ct: CancellationToken) => {
			return new Promise<void>(async (resolve, reject) => {
				if (this._logger.isTraceEnabled) {
					this._logger.trace("Executing:", this._sqlText, values);
				}
				try {
					await executeRunQuery(this._owner.postgresConnection, this._sqlText, values);
					if (this._logger.isTraceEnabled) {
						this._logger.trace("Executed");
					}
					resolve();
				} catch (err) {
					if (this._logger.isTraceEnabled) {
						this._logger.trace("Executed with error:", err);
					}
					return reject(err);
				}
			});
		}, cancellationToken);
	}

	public executeQuery(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<Array<SqlResultRecord>> {
		return Task.run((ct: CancellationToken) => {
			return new Promise<Array<SqlResultRecord>>(async (resolve, reject) => {
				if (this._logger.isTraceEnabled) {
					this._logger.trace("Executing Query:", this._sqlText, values);
				}
				try {
					const underlyingResult = await executeRunQuery(this._owner.postgresConnection, this._sqlText, values);
					const underlyingResultRows = underlyingResult.rows;
					const underlyingResultFields = underlyingResult.fields;
					if (underlyingResultFields[0].dataTypeID === DATA_TYPE_ID_MULTI) {
						return reject(new InvalidOperationError("executeQuery does not support multi request"));
					}
					if (underlyingResultRows.length > 0 && !(underlyingResultFields[0].dataTypeID === DATA_TYPE_ID_EMPTY)) {
						return resolve(underlyingResultRows.map(row => new PostgresSqlResultRecord(row, underlyingResultFields)));
					} else {
						return resolve([]);
					}
				} catch (e) {
					return reject(e);
				}
			});
		}, cancellationToken);
	}

	// tslint:disable-next-line:max-line-length
	public executeQueryMultiSets(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<Array<Array<SqlResultRecord>>> {
		function parsingValue(res: pg.QueryResult): Array<any> {
			const rows = res.rows;
			return rows.map((row) => row[Object.keys(row)[0]]);
		}
		return Task.run((ct: CancellationToken) => {
			return new Promise<Array<Array<SqlResultRecord>>>(async (resolve, reject) => {
				try {
					// Begin transaction
					await executeRunQuery(this._owner.postgresConnection, "BEGIN", []);
					const resultFetchs = await executeRunQuery(this._owner.postgresConnection, this._sqlText, values);
					if (resultFetchs.fields[0].dataTypeID !== DATA_TYPE_ID_MULTI) {
						return reject(new InvalidOperationError(`executeQueryMultiSets cannot execute this script: ${this._sqlText}`));
					}
					const resultFetchsValue = parsingValue(resultFetchs);
					const friendlyResult: Array<Array<SqlResultRecord>> = [];
					for (let i = 0; i < resultFetchsValue.length; i++) {
						const fetch = resultFetchsValue[i];
						const queryFetchs = await executeRunQuery(this._owner.postgresConnection, `FETCH ALL IN "${fetch}";`, []);
						friendlyResult.push(queryFetchs.rows.map(row => new PostgresSqlResultRecord(row, queryFetchs.fields)));
					}
					// Close transaction
					await executeRunQuery(this._owner.postgresConnection, "COMMIT", []);
					return resolve(friendlyResult);
				} catch (err) {
					return reject(err);
				}
			});
		}, cancellationToken);
	}

	public executeScalar(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<SqlData> {
		return Task.run((ct: CancellationToken) => {
			return new Promise<SqlData>((resolve, reject) => {
				if (this._logger.isTraceEnabled) {
					this._logger.trace("Executing Scalar:", this._sqlText, values);
				}
				this._owner.postgresConnection.query(this._sqlText, values, (
					(err: any, underlyingResult: pg.QueryResult) => {
						if (err) {
							if (this._logger.isTraceEnabled) {
								this._logger.trace("Executed Scalar with error:", err);
							}
							return reject(err);
						}
						if (this._logger.isTraceEnabled) {
							this._logger.trace("Executed Scalar:", underlyingResult);
						}
						const underlyingRows = underlyingResult.rows;
						const underlyingFields = underlyingResult.fields;
						if (underlyingRows.length > 0) {
							const underlyingFirstRow = underlyingRows[0];
							const value = underlyingFirstRow[Object.keys(underlyingFirstRow)[0]];
							const fi = underlyingFields[0];
							if (value !== undefined || fi !== undefined) {
								return resolve(new PostgresData(value, fi));
							}
						}
						return reject(new Error("Underlying Postgres provider returns not enough data to complete request."));
					}));
			});
		}, cancellationToken);
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

	private readonly _owner: PostgresProvider;
	private readonly _cancellationToken: CancellationToken;
	private readonly _tableName: string;
	private readonly _columnsDefinitions: string;

	public constructor(owner: PostgresProvider, cancellationToken: CancellationToken, tableName: string, columnsDefinitions: string) {
		super();
		this._owner = owner;
		this._cancellationToken = cancellationToken;
		this._tableName = tableName;
		this._columnsDefinitions = columnsDefinitions;
	}

	public bulkInsert(cancellationToken: CancellationToken, bulkValues: Array<Array<SqlStatementParam>>): TaskLike<void> {
		return this._owner.statement(`INSERT INTO \`${this._tableName}\``).execute(cancellationToken, bulkValues as any);
	}
	public crear(cancellationToken: CancellationToken): TaskLike<void> {
		return this._owner.statement(`DELETE FROM \`${this._tableName}\``).execute(cancellationToken);
	}
	public insert(cancellationToken: CancellationToken, values: Array<SqlStatementParam>): Task<void> {
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
			if (e instanceof Error && e.name === "CancelledError") {
				return; // skip error message if task was cancelled
			}
			// Should never happened
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
	public get asFinancial(): FinancialLike {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		} else if (typeof this._postgresValue === "number") {
			return financial(this._postgresValue, FINACIAL_NUMBER_DEFAULT_FRACTION);
		} else if (typeof this._postgresValue === "string") {
			return financial(this._postgresValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage());
		}
	}
	public get asNullableFinancial(): FinancialLike | null {
		if (this._postgresValue === null) {
			return null;
		} else if (typeof this._postgresValue === "number") {
			return financial(this._postgresValue, FINACIAL_NUMBER_DEFAULT_FRACTION);
		} else if (typeof this._postgresValue === "string") {
			return financial(this._postgresValue);
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
			throw new ArgumentError("postgresValue is undefined");
		}
		this._postgresValue = postgresValue;
		this._fi = fi;
	}

	private formatWrongDataTypeMessage(): string {
		return `Invalid conversion: requested wrong data type of field '${this._fi.name}'`;
	}
}

class DummyLogger implements Logger {
	public get isTraceEnabled(): boolean { return false; }
	public get isDebugEnabled(): boolean { return false; }
	public get isInfoEnabled(): boolean { return false; }
	public get isWarnEnabled(): boolean { return false; }
	public get isErrorEnabled(): boolean { return false; }
	public get isFatalEnabled(): boolean { return false; }

	public trace(message: string, ...args: any[]): void {
		// dummy
	}
	public debug(message: string, ...args: any[]): void {
		// dummy
	}
	public info(message: string, ...args: any[]): void {
		// dummy
	}
	public warn(message: string, ...args: any[]): void {
		// dummy
	}
	public error(message: string, ...args: any[]): void {
		// dummy
	}
	public fatal(message: string, ...args: any[]): void {
		// dummy
	}
}
