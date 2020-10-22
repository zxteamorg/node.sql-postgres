import { CancellationToken, Financial, Logger } from "@zxteam/contract";
import { DUMMY_CANCELLATION_TOKEN } from "@zxteam/cancellation";
import { Disposable, Initable } from "@zxteam/disposable";
import {
	ArgumentError, CancelledError, InvalidOperationError,
	wrapErrorIfNeeded, AggregateError
} from "@zxteam/errors";
import { FinancialOperation, financial } from "@zxteam/financial";
import {
	SqlProviderFactory, SqlProvider, SqlStatement,
	SqlStatementParam, SqlResultRecord, SqlData,
	SqlTemporaryTable, SqlDialect, SqlError,
	SqlSyntaxError, SqlConstraintError, SqlPermissionError, SqlNoSuchRecordError
} from "@zxteam/sql";

import * as _ from "lodash";
import * as pg from "pg";

/**
 * Package oid contains OID constants as defined by the Postgres server.
 * @description Use a query `SELECT concat(typname, ' = ', oid, ',') FROM pg_type WHERE oid < 10000 ORDER BY oid` to generate this enum.
 * @see https://github.com/postgres/postgres/blob/2e4db241bfd3206bad8286f8ffc2db6bbdaefcdf/src/include/catalog/pg_type.dat
 */
const enum PostgresObjectID {
	bool = 16,
	bytea = 17,
	char = 18,
	name = 19,
	int8 = 20,
	int2 = 21,
	int2vector = 22,
	int4 = 23,
	regproc = 24,
	text = 25,
	oid = 26,
	tid = 27,
	xid = 28,
	cid = 29,
	oidvector = 30,
	pg_ddl_command = 32,
	pg_type = 71,
	pg_attribute = 75,
	pg_proc = 81,
	pg_class = 83,
	json = 114,
	xml = 142,
	_xml = 143,
	pg_node_tree = 194,
	_json = 199,
	smgr = 210,
	index_am_handler = 325,
	point = 600,
	lseg = 601,
	path = 602,
	box = 603,
	polygon = 604,
	line = 628,
	_line = 629,
	cidr = 650,
	_cidr = 651,
	float4 = 700,
	float8 = 701,
	abstime = 702,
	reltime = 703,
	tinterval = 704,
	unknown = 705,
	circle = 718,
	_circle = 719,
	money = 790,
	_money = 791,
	macaddr = 829,
	inet = 869,
	_bool = 1000,
	_bytea = 1001,
	_char = 1002,
	_name = 1003,
	_int2 = 1005,
	_int2vector = 1006,
	_int4 = 1007,
	_regproc = 1008,
	_text = 1009,
	_tid = 1010,
	_xid = 1011,
	_cid = 1012,
	_oidvector = 1013,
	_bpchar = 1014,
	_varchar = 1015,
	_int8 = 1016,
	_point = 1017,
	_lseg = 1018,
	_path = 1019,
	_box = 1020,
	_float4 = 1021,
	_float8 = 1022,
	_abstime = 1023,
	_reltime = 1024,
	_tinterval = 1025,
	_polygon = 1027,
	_oid = 1028,
	aclitem = 1033,
	_aclitem = 1034,
	_macaddr = 1040,
	_inet = 1041,
	bpchar = 1042,
	varchar = 1043,
	date = 1082,
	time = 1083,
	timestamp = 1114,
	_timestamp = 1115,
	_date = 1182,
	_time = 1183,
	timestamptz = 1184,
	_timestamptz = 1185,
	interval = 1186,
	_interval = 1187,
	_numeric = 1231,
	pg_database = 1248,
	_cstring = 1263,
	timetz = 1266,
	_timetz = 1270,
	bit = 1560,
	_bit = 1561,
	varbit = 1562,
	_varbit = 1563,
	numeric = 1700,
	refcursor = 1790,
	_refcursor = 2201,
	regprocedure = 2202,
	regoper = 2203,
	regoperator = 2204,
	regclass = 2205,
	regtype = 2206,
	_regprocedure = 2207,
	_regoper = 2208,
	_regoperator = 2209,
	_regclass = 2210,
	_regtype = 2211,
	record = 2249,
	cstring = 2275,
	any = 2276,
	anyarray = 2277,
	void = 2278,
	trigger = 2279,
	language_handler = 2280,
	internal = 2281,
	opaque = 2282,
	anyelement = 2283,
	_record = 2287,
	anynonarray = 2776,
	pg_authid = 2842,
	pg_auth_members = 2843,
	_txid_snapshot = 2949,
	uuid = 2950,
	_uuid = 2951,
	txid_snapshot = 2970,
	fdw_handler = 3115,
	pg_lsn = 3220,
	_pg_lsn = 3221,
	tsm_handler = 3310,
	anyenum = 3500,
	tsvector = 3614,
	tsquery = 3615,
	gtsvector = 3642,
	_tsvector = 3643,
	_gtsvector = 3644,
	_tsquery = 3645,
	regconfig = 3734,
	_regconfig = 3735,
	regdictionary = 3769,
	_regdictionary = 3770,
	jsonb = 3802,
	_jsonb = 3807,
	anyrange = 3831,
	event_trigger = 3838,
	int4range = 3904,
	_int4range = 3905,
	numrange = 3906,
	_numrange = 3907,
	tsrange = 3908,
	_tsrange = 3909,
	tstzrange = 3910,
	_tstzrange = 3911,
	daterange = 3912,
	_daterange = 3913,
	int8range = 3926,
	_int8range = 3927,
	pg_shseclabel = 4066,
	regnamespace = 4089,
	_regnamespace = 4090,
	regrole = 4096,
	_regrole = 4097
}

export class PostgresProviderFactory extends Initable implements SqlProviderFactory {
	private readonly _financialOperation: FinancialOperation;
	private readonly _log: Logger;
	private readonly _url: URL;
	private readonly _pool: pg.Pool;
	private readonly _defaultSchema: string;

	// This implemenation wrap package https://www.npmjs.com/package/pg
	public constructor(opts: PostgresProviderFactory.Opts) {
		super();

		switch (opts.url.protocol) {
			case "postgres:":
			case "postgres+ssl:":
				this._url = opts.url;
				break;
			default:
				throw new ArgumentError("opts.url", "Expected URL schema 'postgres:' or 'postgres+ssl:'.");
		}

		this._log = opts.log !== undefined ? opts.log : DUMMY_LOGGER;
		this._log.trace("PostgresProviderPoolFactory constructed");

		this._financialOperation = opts.financialOperation !== undefined ? opts.financialOperation : financial;

		const poolConfig: pg.PoolConfig = { host: this._url.hostname };

		if (!_.isEmpty(this._url.port)) { poolConfig.port = Number.parseInt(this._url.port); }
		if (!_.isEmpty(this._url.username)) { poolConfig.user = this._url.username; }
		if (!_.isEmpty(this._url.password)) { poolConfig.password = this._url.password; }

		// DB name
		let pathname = this._url.pathname;
		while (pathname.length > 0 && pathname[0] === "/") { pathname = pathname.substring(1); }
		poolConfig.database = pathname;

		// Timeouts
		poolConfig.connectionTimeoutMillis = (opts.connectionTimeoutMillis !== undefined) ? opts.connectionTimeoutMillis : 5000;
		poolConfig.idleTimeoutMillis = (opts.idleTimeoutMillis !== undefined) ? opts.idleTimeoutMillis : 30000;

		// Keep-alive
		poolConfig.keepAlive = (opts.keepAlive !== undefined) ? opts.keepAlive : true;
		poolConfig.keepAliveInitialDelayMillis = (opts.keepAliveInitialDelayMillis !== undefined) ? opts.keepAliveInitialDelayMillis : 5000;

		// App name
		if (!_.isEmpty(opts.applicationName)) {
			poolConfig.application_name = opts.applicationName;
		} else {
			const appNameFromUrl: string | null = this._url.searchParams.get("app");
			if (appNameFromUrl !== null && !_.isEmpty(appNameFromUrl)) {
				poolConfig.application_name = appNameFromUrl;
			}
		}

		const schemaFromUrl: string | null = this._url.searchParams.get("schema");
		this._defaultSchema = opts.defaultSchema !== undefined ?
			opts.defaultSchema :
			schemaFromUrl !== null ? schemaFromUrl : "public";

		// SSL
		if (this._url.protocol === "postgres+ssl:") {
			poolConfig.ssl = {};
			if (opts.ssl !== undefined) {
				if (opts.ssl !== undefined) {
					if (opts.ssl === "prefer") {
						poolConfig.ssl.rejectUnauthorized = false;
						this._log.debug("Using partial secured connection without checking identity (SSL-prefer, no certificate validation).");
					} else {
						poolConfig.ssl.rejectUnauthorized = true;
						this._log.debug("Using full secured connection (with certificate validation).");
						if (opts.ssl.caCert !== undefined) {
							poolConfig.ssl.ca = opts.ssl.caCert;
							this._log.debug("CA certificate was provided.");
						}
						if (opts.ssl.clientCert !== undefined) {
							poolConfig.ssl.cert = opts.ssl.clientCert.cert;
							poolConfig.ssl.key = opts.ssl.clientCert.key;
							this._log.debug("Client certificate was provided.");
						}
					}
				}
			} else {
				poolConfig.ssl.rejectUnauthorized = false;
				this._log.debug("Using partial secured connection without checking identity (SSL-prefer, no certificate validation).");
			}
		} else {
			this._log.debug("Using insecured connection (non-SSL).");
		}

		this._pool = new pg.Pool(poolConfig);
		this._pool.on("error", (err: Error, connection: pg.PoolClient) => {
			/*
				https://node-postgres.com/api/pool
				When a client is sitting idly in the pool it can still emit errors
				because it is connected to a live backend. If the backend goes down
				or a network partition is encountered all the idle, connected clients
				in your application will emit an error through the pool's error event emitter.
				The error listener is passed the error as the first argument and the client
				upon which the error occurred as the 2nd argument. The client will be
				automatically terminated and removed from the pool, it is only passed to the
				error handler in case you want to inspect it.
			 */
			this._log.debug(err.message);
			this._log.trace(err.message, err);
		});
	}

	public get defaultSchema(): string { return this._defaultSchema; }

	public async create(cancellationToken: CancellationToken): Promise<SqlProvider> {
		this.verifyInitializedAndNotDisposed();

		const pgClient = await this._pool.connect();
		try {
			cancellationToken.throwIfCancellationRequested();

			if (this._defaultSchema !== null) {
				await pgClient.query(`SET search_path TO ${this._defaultSchema}`);
			}

			await pgClient.query("SET TIME ZONE '00:00'");

			const sqlProvider: SqlProvider = new PostgresSqlProvider(
				pgClient,
				async () => {
					// dispose callback
					pgClient.release();
				},
				this._financialOperation,
				this._log
			);

			return sqlProvider;
		} catch (e) {
			pgClient.release();
			throw e;
		}
	}

	public usingProvider<T>(
		cancellationToken: CancellationToken,
		worker: (sqlProvder: SqlProvider) => T | Promise<T>
	): Promise<T> {
		const executionPromise: Promise<T> = (async () => {
			const sqlProvider: SqlProvider = await this.create(cancellationToken);
			try {
				return await worker(sqlProvider);
			} finally {
				await sqlProvider.dispose();
			}
		})();

		return executionPromise;
	}

	public usingProviderWithTransaction<T>(
		cancellationToken: CancellationToken, worker: (sqlProvder: SqlProvider) => T | Promise<T>
	): Promise<T> {
		return this.usingProvider(cancellationToken, async (sqlProvider: SqlProvider) => {
			await sqlProvider.statement("BEGIN TRANSACTION").execute(cancellationToken);
			try {
				let result: T;
				const workerResult = worker(sqlProvider);
				if (workerResult instanceof Promise) {
					result = await workerResult;
				} else {
					result = workerResult;
				}
				// We have not to cancel this operation, so pass DUMMY_CANCELLATION_TOKEN
				await sqlProvider.statement("COMMIT TRANSACTION").execute(DUMMY_CANCELLATION_TOKEN);
				return result;
			} catch (e) {
				try {
					// We have not to cancel this operation, so pass DUMMY_CANCELLATION_TOKEN
					await sqlProvider.statement("ROLLBACK TRANSACTION").execute(DUMMY_CANCELLATION_TOKEN);
				} catch (e2) {
					throw new AggregateError([e, e2]);
				}
				throw e;
			}
		});
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
		/**
		 * Default schema. The value overrides an URL param "schema".
		 * @description Each pgClient will execute SQL statement: `SET search_path TO ${defaultSchema}` before wrapping in `PostgresSqlProvider`
		 * @default "public"
		 */
		readonly defaultSchema?: string;
		/**
		 * Application name. Used by Postres in monitoring stuff.
		 * The value ovverides an URL param "app"
		 */
		readonly applicationName?: string;
		readonly log?: Logger;
		/**
		 * @default 5000
		 */
		readonly connectionTimeoutMillis?: number;
		/**
		 * @default 30000
		 */
		readonly idleTimeoutMillis?: number;

		/**
		 * @default true
		 */
		keepAlive?: boolean;

		/**
		 * @default 5000
		 */
		keepAliveInitialDelayMillis?: number;

		readonly financialOperation?: FinancialOperation;
		readonly ssl?: "prefer" | {
			readonly caCert?: Buffer;
			readonly clientCert?: {
				readonly cert: Buffer;
				readonly key: Buffer;
			}
		};
	}
}

class PostgresSqlProvider extends Disposable implements SqlProvider {
	public readonly financialOperation: FinancialOperation;
	public readonly dialect: SqlDialect = SqlDialect.PostgreSQL;
	public readonly pgClient: pg.PoolClient;
	public readonly log: Logger;
	private readonly _disposer: () => Promise<void>;
	public constructor(pgClient: pg.PoolClient, disposer: () => Promise<void>, financialOperation: FinancialOperation, log: Logger) {
		super();
		this.pgClient = pgClient;
		this._disposer = disposer;
		this.financialOperation = financialOperation;
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
		await this._disposer();
	}
}

class PostgresSqlStatement implements SqlStatement {
	private readonly _sqlText: string;
	private readonly _owner: PostgresSqlProvider;

	public constructor(owner: PostgresSqlProvider, sql: string) {
		this._owner = owner;
		this._sqlText = sql;
	}

	public async execute(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Promise<void> {
		await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);
	}

	public async executeQuery(
		cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>
	): Promise<Array<SqlResultRecord>> {
		const underlyingResult = await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);

		const underlyingResultRows = underlyingResult.rows;
		const underlyingResultFields = underlyingResult.fields;

		if (underlyingResultFields[0].dataTypeID === PostgresObjectID.refcursor) {
			throw new InvalidOperationError("executeQuery: does not support multiset request yet");
		}

		if (underlyingResultRows.length > 0 && !(underlyingResultFields[0].dataTypeID === PostgresObjectID.void)) {
			return underlyingResultRows.map(row => new PostgresSqlResultRecord(row, underlyingResultFields, this._owner.financialOperation));
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
				helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
			);
			cancellationToken.throwIfCancellationRequested();

			// Verify that this is a multi-request
			if (resultFetchs.fields[0].dataTypeID !== PostgresObjectID.refcursor) {
				// This is not a multi request. Raise exception.
				throw new InvalidOperationError(`executeQueryMultiSets: cannot execute this script: ${this._sqlText}`);
			}

			const resultFetchsValue = helpers.parsingValue(resultFetchs);
			const friendlyResult: Array<Array<SqlResultRecord>> = [];
			for (let i = 0; i < resultFetchsValue.length; i++) {
				const fetch = resultFetchsValue[i];

				const queryFetchs = await helpers.executeRunQuery(cancellationToken, this._owner.pgClient, `FETCH ALL IN "${fetch}";`, []);
				cancellationToken.throwIfCancellationRequested();

				friendlyResult.push(queryFetchs.rows.map(row => new PostgresSqlResultRecord(row, queryFetchs.fields, this._owner.financialOperation)));
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
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);


		const underlyingRows = underlyingResult.rows;
		if (underlyingRows.length === 0) {
			throw new SqlNoSuchRecordError(`executeScalar: No record for query ${this._sqlText}`);
		}

		const underlyingFields = underlyingResult.fields;

		if (underlyingFields.length === 0) {
			throw new InvalidOperationError("executeScalar: SQL query returns no result");
		}

		if (underlyingFields[0].dataTypeID === PostgresObjectID.refcursor) {
			throw new InvalidOperationError("executeScalar: does not support multiset request yet");
		}

		const underlyingFirstRow = underlyingRows[0];
		const value = underlyingFirstRow[Object.keys(underlyingFirstRow)[0]];
		const fi = underlyingFields[0];
		if (value !== undefined || fi !== undefined) {
			return new PostgresData(value, fi, this._owner.financialOperation);
		} else {
			throw new SqlError(`executeScalar: Bad argument ${value} and ${fi}`);
		}
	}

	public async executeScalarOrNull(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Promise<SqlData | null> {
		const underlyingResult = await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);

		const underlyingRows = underlyingResult.rows;
		const underlyingFields = underlyingResult.fields;
		if (underlyingRows.length > 0) {
			if (underlyingFields[0].dataTypeID === PostgresObjectID.refcursor) {
				throw new InvalidOperationError("executeScalarOrNull: does not support multiset request yet");
			}

			const underlyingFirstRow = underlyingRows[0];
			const value = underlyingFirstRow[Object.keys(underlyingFirstRow)[0]];
			const fi = underlyingFields[0];
			if (value !== undefined || fi !== undefined) {
				return new PostgresData(value, fi, this._owner.financialOperation);
			} else {
				throw new SqlError(`executeScalarOrNull: Bad argument ${value} and ${fi}`);
			}
		} else {
			return null;
		}
	}

	public async executeSingle(cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>): Promise<SqlResultRecord> {
		const underlyingResult = await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);

		const underlyingResultRows = underlyingResult.rows;
		const underlyingResultFields = underlyingResult.fields;

		if (underlyingResultFields.length === 0) {
			throw new InvalidOperationError("executeSingle: SQL query returns no result");
		}

		if (underlyingResultFields[0].dataTypeID === PostgresObjectID.refcursor) {
			throw new InvalidOperationError("executeSingle: does not support multi request");
		}

		if (underlyingResultRows.length === 0) {
			throw new SqlNoSuchRecordError(`executeSingle: No record for query ${this._sqlText}`);
		} else if (underlyingResultRows.length === 1 && !(underlyingResultFields[0].dataTypeID === PostgresObjectID.void)) {
			return new PostgresSqlResultRecord(underlyingResultRows[0], underlyingResultFields, this._owner.financialOperation);
		} else {
			throw new InvalidOperationError(`executeSingle: SQL query returns non-single result`);
		}
	}

	public async executeSingleOrNull(
		cancellationToken: CancellationToken, ...values: Array<SqlStatementParam>
	): Promise<SqlResultRecord | null> {
		const underlyingResult = await helpers.executeRunQuery(
			cancellationToken,
			this._owner.pgClient,
			this._sqlText,
			helpers.statementArgumentsAdapter(this._owner.financialOperation, values)
		);

		const underlyingResultRows = underlyingResult.rows;
		const underlyingResultFields = underlyingResult.fields;

		if (underlyingResultFields.length === 0) {
			throw new InvalidOperationError("executeSingleOrNull: SQL query returns no result");
		}

		if (underlyingResultFields[0].dataTypeID === PostgresObjectID.refcursor) {
			throw new InvalidOperationError("executeSingleOrNull: does not support multi request");
		}

		if (underlyingResultRows.length === 0) {
			return null;
		} else if (underlyingResultRows.length === 1 && !(underlyingResultFields[0].dataTypeID === PostgresObjectID.void)) {
			return new PostgresSqlResultRecord(underlyingResultRows[0], underlyingResultFields, this._owner.financialOperation);
		} else {
			throw new InvalidOperationError("executeSingleOrNull: SQL query returns non-single result");
		}
	}
}

namespace PostgresSqlResultRecord {
	export type NameMap = {
		[name: string]: pg.FieldDef;
	};
}
class PostgresSqlResultRecord implements SqlResultRecord {
	private readonly _financialOperation: FinancialOperation;
	private readonly _fieldsData: any;
	private readonly _fieldsInfo: Array<pg.FieldDef>;
	private _nameMap?: PostgresSqlResultRecord.NameMap;

	public constructor(fieldsData: any, fieldsInfo: Array<pg.FieldDef>, financialOperation: FinancialOperation) {
		if (Object.keys(fieldsData).length !== fieldsInfo.length) {
			throw new Error("Internal error. Fields count is not equal to data columns.");
		}
		this._fieldsData = fieldsData;
		this._fieldsInfo = fieldsInfo;
		this._financialOperation = financialOperation;
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
		if (fi === undefined) {
			throw new ArgumentError("index", `PostgresSqlResultRecord does not have field with index '${index}'`);
		}
		const value: any = this._fieldsData[fi.name];
		return new PostgresData(value, fi, this._financialOperation);
	}
	private getByName(name: string): SqlData {
		const fi = this.nameMap[name];
		if (fi === undefined) {
			throw new ArgumentError("name", `PostgresSqlResultRecord does not have field with name '${name}'`);
		}
		const value: any = this._fieldsData[fi.name];
		return new PostgresData(value, fi, this._financialOperation);
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
	private readonly _financialOperation: FinancialOperation;
	private readonly _postgresValue: any;
	private readonly _fi: pg.FieldDef;

	public get asBoolean(): boolean {
		if (typeof this._postgresValue === "boolean") {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asBoolean"));
		}
	}
	public get asNullableBoolean(): boolean | null {
		if (this._postgresValue === null) {
			return null;
		} else if (typeof this._postgresValue === "boolean") {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asNullableBoolean"));
		}
	}
	public get asString(): string {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asString"));
		} else if (_.isString(this._postgresValue)) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asString"));
		}
	}
	public get asNullableString(): string | null {
		if (this._postgresValue === null) {
			return null;
		} else if (_.isString(this._postgresValue)) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asNullableString"));
		}
	}
	public get asInteger(): number {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asInteger"));
		} else if (_.isNumber(this._postgresValue) && Number.isInteger(this._postgresValue)) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asInteger"));
		}
	}
	public get asNullableInteger(): number | null {
		if (this._postgresValue === null) {
			return null;
		} else if (_.isNumber(this._postgresValue) && Number.isInteger(this._postgresValue)) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asNullableInteger"));
		}
	}
	public get asNumber(): number {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asNumber"));
		} else if (_.isNumber(this._postgresValue)) {
			return this._postgresValue;
		} else if (this._fi.dataTypeID === PostgresObjectID.numeric && _.isString(this._postgresValue)) {
			return Number.parseFloat(this._postgresValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asNumber"));
		}
	}
	public get asNullableNumber(): number | null {
		if (this._postgresValue === null) {
			return null;
		} else if (_.isNumber(this._postgresValue)) {
			return this._postgresValue;
		} else if (this._fi.dataTypeID === PostgresObjectID.numeric && _.isString(this._postgresValue)) {
			return Number.parseFloat(this._postgresValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asNullableNumber"));
		}
	}
	public get asFinancial(): Financial {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asFinancial"));
		} else if (_.isNumber(this._postgresValue)) {
			return this._financialOperation.fromFloat(this._postgresValue);
		} else if (_.isString(this._postgresValue)) {
			return this._financialOperation.parse(this._postgresValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asFinancial"));
		}
	}
	public get asNullableFinancial(): Financial | null {
		if (this._postgresValue === null) {
			return null;
		} else if (_.isNumber(this._postgresValue)) {
			return this._financialOperation.fromFloat(this._postgresValue);
		} else if (_.isString(this._postgresValue)) {
			return this._financialOperation.parse(this._postgresValue);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asNullableFinancial"));
		}
	}
	public get asDate(): Date {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asDate"));
		} else if (this._fi.dataTypeID === PostgresObjectID.timestamp && this._postgresValue instanceof Date) {
			// `pg` library make Date with local zone shift, so we need to make oposite changes to retrieve correct date from UTC timestamp
			return new Date(this._postgresValue.getTime() - this._postgresValue.getTimezoneOffset() * 60000);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage(
				"asDate",
				`Right now the library supports TIMESTAMP WITHOUT TIME ZONE OID=${PostgresObjectID.timestamp} only. Got OID=${this._fi.dataTypeID}.`
			));
		}
	}
	public get asNullableDate(): Date | null {
		if (this._postgresValue === null) {
			return null;
		} else if (this._fi.dataTypeID === PostgresObjectID.timestamp && this._postgresValue instanceof Date) {
			// `pg` library make Date with local zone shift, so we need to make oposite changes to retrieve correct date from UTC timestamp
			return new Date(this._postgresValue.getTime() - this._postgresValue.getTimezoneOffset() * 60000);
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage(
				"asNullableDate",
				`Right now the library supports TIMESTAMP WITHOUT TIME ZONE OID=${PostgresObjectID.timestamp} only. Got OID=${this._fi.dataTypeID}.`
			));
		}
	}
	public get asBinary(): Uint8Array {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asBinary"));
		} else if (this._postgresValue instanceof Uint8Array) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asBinary"));
		}
	}
	public get asNullableBinary(): Uint8Array | null {
		if (this._postgresValue === null) {
			return null;
		} else if (this._postgresValue instanceof Uint8Array) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asNullableBinary"));
		}
	}
	public get asObject(): any {
		if (this._postgresValue === null) {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asObject"));
		} else if (this._fi.dataTypeID === PostgresObjectID.jsonb) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage(
				"asObject",
				`Right now the library supports Binary JSON (OID=${PostgresObjectID.jsonb}) only.`
			));
		}
	}
	public get asNullableObject(): any | null {
		if (this._postgresValue === null) {
			return null;
		} else if (this._fi.dataTypeID === PostgresObjectID.jsonb) {
			return this._postgresValue;
		} else {
			throw new InvalidOperationError(this.formatWrongDataTypeMessage("asNullableObject"));
		}
	}

	public constructor(postgresValue: any, fi: pg.FieldDef, financialOperation: FinancialOperation) {
		if (postgresValue === undefined) {
			throw new ArgumentError("postgresValue");
		}
		this._postgresValue = postgresValue;
		this._fi = fi;
		this._financialOperation = financialOperation;
	}

	private formatWrongDataTypeMessage(caller: keyof SqlData, subMessage?: string): string {
		let valueConstructorName: string;
		if (this._postgresValue !== null && this._postgresValue !== undefined) {
			valueConstructorName = this._postgresValue.constructor.name;
		} else {
			valueConstructorName = `${this._postgresValue}`;
		}
		const message = `Invalid conversion for caller '${caller}' on a field '${this._fi.name}' with dataTypeID: '${this._fi.dataTypeID}'. PostgresValue instance of constructor '${valueConstructorName}'`;

		if (subMessage !== undefined) {
			return `${message} ${subMessage}`;
		}

		return message;
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

	getLogger(name?: string): Logger { /* NOP */ return DUMMY_LOGGER; }
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
	export async function executeRunQuery(
		cancellationToken: CancellationToken, db: pg.PoolClient, sqlText: string, values: Array<SqlStatementParam>
	): Promise<pg.QueryResult> {
		try {
			return await new Promise<pg.QueryResult>((resolve, reject) => {
				db.query(sqlText, values,
					(err: any, underlyingResult: pg.QueryResult) => {
						if (err) {
							return reject(err);
						}
						return resolve(underlyingResult);
					});
			});
		} catch (reason) {
			const err = wrapErrorIfNeeded(reason);

			if ("code" in reason) {
				const code = reason.code;
				// https://www.postgresql.org/docs/12/errcodes-appendix.html
				switch (code) {
					case "21000":
					case "23000":
					case "23001":
					case "23502":
					case "23503":
					case "23505":
					case "23514":
					case "23P01":
					case "27000":
					case "40002":
					case "42000":
					case "44000":
						throw new SqlConstraintError(
							`SQL Constraint restriction happened. Query: ${sqlText}. Reason Message: ${err.message}. See innerError for details.`,
							_.isString(reason.constraint) ? reason.constraint : "???",
							err
						);
					case "42501":
						throw new SqlPermissionError(`Insufficient permission to execute a query. Query: ${sqlText}. Reason Message: ${err.message}. See innerError for details.`, err);
					case "42000":
					case "42601":
						throw new SqlSyntaxError(`Looks like wrong SQL syntax detected. Query: ${sqlText}. Reason Message: ${err.message}. See innerError for details.`, err);
				}
			}
			throw new SqlError(`Unexpected error occurs while executing a query. Query: ${sqlText}. Reason Message: ${err.message}. See innerError for details.`, err);
		}
	}
	export function statementArgumentsAdapter(financialOperation: FinancialOperation, args: Array<SqlStatementParam>): Array<any> {
		return args.map(value => {
			if (typeof value === "object") {
				if (value !== null) {
					if (financialOperation.isFinancial(value)) {
						return value.toString(); // Financial should be converted to string
					} else if (value instanceof Date) {
						// `pg` library make Date with local zone shift, so we need to make oposite changes to save correct date as UTC timestamp
						return new Date(value.getTime() + value.getTimezoneOffset() * 60000);
					}
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
