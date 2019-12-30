import { MigrationManager, SqlProvider } from "@zxteam/sql";
import { CancellationToken } from "@zxteam/contract";

import { PostgresProviderFactory } from "./PostgresProviderFactory";

export class PostgresMigrationManager extends MigrationManager {
	private readonly _schema: string;

	public constructor(opts: PostgresMigrationManager.Opts) {
		super(opts);
		this._schema = opts.sqlProviderFactory.defaultSchema;
	}

	public getCurrentVersion(cancellationToken: CancellationToken): Promise<string | null> {
		return this.sqlProviderFactory.usingProvider(cancellationToken, async (sqlProvider: SqlProvider) => {

			const isExist = await this._isVersionTableExist(cancellationToken, sqlProvider);
			if (isExist === false) { return null; }

			await this._verifyVersionTableStructure(cancellationToken, sqlProvider);

			const versionData = await sqlProvider.statement(
				`SELECT "version" FROM "${this.versionTableName}" ORDER BY "version" DESC LIMIT 1`
			).executeScalarOrNull(cancellationToken);

			if (versionData === null) {
				return null;
			}

			return versionData.asString;
		});
	}

	protected async _isVersionTableExist(cancellationToken: CancellationToken, sqlProvider: SqlProvider): Promise<boolean> {
		const isExistSqlData = await sqlProvider.statement(
			`SELECT 1 FROM "pg_catalog"."pg_tables" WHERE "schemaname" != 'pg_catalog' AND "schemaname" != 'information_schema' AND "schemaname" = $1 AND "tablename" = $2`
		).executeScalarOrNull(cancellationToken, this._schema, this.versionTableName);

		if (isExistSqlData === null) { return false; }
		if (isExistSqlData.asInteger !== 1) { throw new PostgresMigrationManager.MigrationError("Unexpected SQL result"); }

		return true;
	}

	protected async _verifyVersionTableStructure(cancellationToken: CancellationToken, sqlProvider: SqlProvider): Promise<void> {
		const isExist = await this._isVersionTableExist(cancellationToken, sqlProvider);
		if (isExist === false) { throw new PostgresMigrationManager.MigrationError(`The database does not have version table: ${this.versionTableName}`); }

		// TODO check columns
		// It is hard to check without schema name
		// SELECT * FROM information_schema.columns WHERE table_schema = '????' AND table_name = '${this.versionTableName}'
	}

	protected async _createVersionTable(cancellationToken: CancellationToken, sqlProvider: SqlProvider): Promise<void> {
		await sqlProvider.statement(`CREATE SCHEMA IF NOT EXISTS "${this._schema}"`).execute(cancellationToken);

		const tables = await sqlProvider.statement(
			`SELECT "tablename" FROM "pg_catalog"."pg_tables" WHERE "schemaname" != 'pg_catalog' AND "schemaname" != 'information_schema' AND "schemaname" = $1 AND "tablename" != 'emptytestflag'`
		).executeQuery(cancellationToken, this._schema);
		if (tables.length > 0) {
			const tablesString = tables.slice(0, 5).map(sqlData => sqlData.get(0).asString).join(", ") + "..";
			throw new PostgresMigrationManager.MigrationError(`Your database has tables: ${tablesString}. Create Version Table allowed only for an empty database. Please create Version Table yourself.`);
		}

		const views = await sqlProvider.statement(
			`SELECT "viewname" FROM "pg_catalog"."pg_views" WHERE "schemaname" != 'pg_catalog' AND "schemaname" != 'information_schema' AND "schemaname" = $1`
		).executeQuery(cancellationToken, this._schema);
		if (views.length > 0) {
			const viewsString = views.slice(0, 5).map(sqlData => sqlData.get(0).asString).join(", ") + "..";
			throw new PostgresMigrationManager.MigrationError(`Your database has views: ${viewsString}. Create Version Table allowed only for an empty database. Please create Version Table yourself.`);
		}

		await sqlProvider.statement(
			`CREATE TABLE "${this.versionTableName}" (` +
			`"version" VARCHAR(64) NOT NULL PRIMARY KEY, ` +
			`"utc_deployed_at" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT(now() AT TIME ZONE 'utc'), ` +
			`"log" TEXT NOT NULL`
			+ `)`
		).execute(cancellationToken);
	}

	protected async _insertVersionLog(
		cancellationToken: CancellationToken, sqlProvider: SqlProvider, version: string, logText: string
	): Promise<void> {
		await sqlProvider.statement(
			`INSERT INTO "${this.versionTableName}"("version", "log") VALUES($1, $2)`
		).execute(cancellationToken, version, logText);
	}
}

export namespace PostgresMigrationManager {
	export interface Opts extends MigrationManager.Opts {
		readonly sqlProviderFactory: PostgresProviderFactory;
	}
}
