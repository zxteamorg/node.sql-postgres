import { Factory, Logger, CancellationToken, Task as TaskLike, Financial as FinancialLike } from "@zxteam/contract";
import {
	SqlProviderFactory, SqlProvider, SqlStatement, SqlStatementParam, SqlResultRecord, SqlData, SqlTemporaryTable
} from "@zxteam/contract.sql";
import { Disposable, Initable } from "@zxteam/disposable";
import { financial, Financial } from "@zxteam/financial.js";
import { Task, CancelledError } from "ptask.js";
import { URL } from "url";
import * as pg from "pg";
const { Client } = require("pg");


const FINACIAL_NUMBER_DEFAULT_FRACTION = 12;
const DATA_TYPE_ID_EMPTY = 2278; // Return postgres if data is null
const DATA_TYPE_ID_MULTI = 1790; // Return postgres if data is multy


export class PostgresProviderFactory implements SqlProviderFactory {
	private readonly _logger: Logger;
	private readonly _url: URL;

	// This implemenation wrap package https://www.npmjs.com/package/pg
	public constructor(url: URL, logger?: Logger) {
		this._logger = logger || new DummyLogger();
		this._url = url;

		this._logger.trace("PostgresProviderFactory Constructed");
	}

	public create(cancellationToken?: CancellationToken): Task<SqlProvider> {
		return Task.run(async (ct) => {
			this._logger.trace("Creating Postgres SqlProvider..");

			this._logger.trace("Check cancellationToken for interrupt");
			if (ct.isCancellationRequested) { throw new CancelledError(); }

			if (this._logger.isTraceEnabled) {
				this._logger.trace(`Opening the database url: ${this._url}`);
			}
			const client = await helpers.openDatabase(this._url);

			try {
				this._logger.trace("Check cancellationToken for interrupt");
				ct.throwIfCancellationRequested();

				const sqlProvider: SqlProvider = new PostgresProvider(
					client,
					() => helpers.closeDatabase(client),
					this._logger
				);

				if (this._logger.isTraceEnabled) {
					this._logger.trace(`The database url ${this._url} was opened successfully`);
				}

				return sqlProvider;
			} catch (e) {
				await helpers.closeDatabase(client);
				throw e;
			}
		}, cancellationToken);
	}
}

export default PostgresProviderFactory;

class ArgumentError extends Error { }
class InvalidOperationError extends Error { }

class PostgresProvider extends Disposable implements SqlProvider {
	public readonly postgresConnection: pg.Client;
	private readonly _logger: Logger;
	private readonly _disposer: () => Promise<void>;
	public constructor(postgresConnection: pg.Client, disposer: () => Promise<void>, logger: Logger) {
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
		return Task.run(async (ct: CancellationToken) => {
			if (this._logger.isTraceEnabled) {
				this._logger.trace("Executing:", this._sqlText, values);
			}
			await helpers.executeRunQuery(
				this._owner.postgresConnection,
				this._sqlText,
				helpers.statementArgumentsAdapter(values)
			);
			if (this._logger.isTraceEnabled) {
				this._logger.trace("Executed");
			}
		}, cancellationToken);
	}

	public executeQuery(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<Array<SqlResultRecord>> {
		return Task.run(async (ct: CancellationToken) => {
			if (this._logger.isTraceEnabled) {
				this._logger.trace("Executing Query:", this._sqlText, values);
			}

			const underlyingResult = await helpers.executeRunQuery(
				this._owner.postgresConnection,
				this._sqlText,
				helpers.statementArgumentsAdapter(values)
			);

			if (this._logger.isTraceEnabled) {
				this._logger.trace("Executed Scalar:", underlyingResult);
			}

			const underlyingResultRows = underlyingResult.rows;
			const underlyingResultFields = underlyingResult.fields;

			this._logger.trace("Verify that this is not a multi-request");
			if (underlyingResultFields[0].dataTypeID === DATA_TYPE_ID_MULTI) {
				this._logger.trace("This is a multi request. Raise exception.");
				throw new InvalidOperationError("executeQuery does not support multi request");
			}

			if (underlyingResultRows.length > 0 && !(underlyingResultFields[0].dataTypeID === DATA_TYPE_ID_EMPTY)) {
				this._logger.trace("Result create new SQLiteSqlResultRecord()");
				return underlyingResultRows.map(row => new PostgresSqlResultRecord(row, underlyingResultFields));
			} else {
				this._logger.trace("Result is empty");
				return [];
			}
		}, cancellationToken);
	}

	// tslint:disable-next-line:max-line-length
	public executeQueryMultiSets(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<Array<Array<SqlResultRecord>>> {
		return Task.run(async (ct: CancellationToken) => {

			this._logger.trace("Executing Query: BEGIN, []");
			await helpers.executeRunQuery(this._owner.postgresConnection, "BEGIN", []);

			this._logger.trace("Check cancellationToken for interrupt");
			ct.throwIfCancellationRequested();

			if (this._logger.isTraceEnabled) {
				this._logger.trace("Executing Scalar:", this._sqlText, values);
			}

			const resultFetchs = await helpers.executeRunQuery(
				this._owner.postgresConnection,
				this._sqlText,
				helpers.statementArgumentsAdapter(values)
			);

			this._logger.trace("Check cancellationToken for interrupt");
			ct.throwIfCancellationRequested();

			this._logger.trace("Verify that this is a multi-request");
			if (resultFetchs.fields[0].dataTypeID !== DATA_TYPE_ID_MULTI) {
				this._logger.trace("This is not a multi request. Raise exception.");
				throw new InvalidOperationError(`executeQueryMultiSets cannot execute this script: ${this._sqlText}`);
			}

			const resultFetchsValue = helpers.parsingValue(resultFetchs);
			const friendlyResult: Array<Array<SqlResultRecord>> = [];
			for (let i = 0; i < resultFetchsValue.length; i++) {
				const fetch = resultFetchsValue[i];

				if (this._logger.isTraceEnabled) {
					this._logger.trace("Executing Scalar:", `FETCH ALL IN "${fetch}";`);
				}

				const queryFetchs = await helpers.executeRunQuery(this._owner.postgresConnection, `FETCH ALL IN "${fetch}";`, []);

				this._logger.trace("Check cancellationToken for interrupt");
				ct.throwIfCancellationRequested();

				friendlyResult.push(queryFetchs.rows.map(row => new PostgresSqlResultRecord(row, queryFetchs.fields)));
			}

			this._logger.trace("Executing Scalar: COMMIT;");
			await helpers.executeRunQuery(this._owner.postgresConnection, "COMMIT", []);

			if (this._logger.isTraceEnabled) {
				this._logger.trace("return friendly data: ", friendlyResult);
			}

			return friendlyResult;
		}, cancellationToken);
	}

	public executeScalar(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Task<SqlData> {
		return Task.run(async (ct: CancellationToken) => {
			if (this._logger.isTraceEnabled) {
				this._logger.trace("Executing Scalar:", this._sqlText, values);
			}

			const underlyingResult = await helpers.executeRunQuery(
				this._owner.postgresConnection,
				this._sqlText,
				helpers.statementArgumentsAdapter(values)
			);

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
					return new PostgresData(value, fi);
				} else {
					if (this._logger.isTraceEnabled) {
						this._logger.trace(`Bad argument ${value} and ${fi}`);
					}
					throw new ArgumentError(`Bad argument ${value} and ${fi}`);
				}
			} else {
				this._logger.trace("Underlying Postgres provider returns not enough data to complete request. Raise exception.");
				throw new Error("Underlying Postgres provider returns not enough data to complete request.");
			}
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

namespace helpers {
	export function openDatabase(url: URL): Promise<pg.Client> {
		return new Promise((resolve, reject) => {
			const client: pg.Client = new Client({
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
	export function executeRunQuery(db: pg.Client, sqlText: string, values: Array<SqlStatementParam>): Promise<pg.QueryResult> {
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
				if (value !== null && Financial.isFinancialLike(value)) {
					return Financial.toString(value);	 // Financial should be converted to string
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
