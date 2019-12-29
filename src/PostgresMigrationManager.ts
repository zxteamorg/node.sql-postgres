import { MigrationManager, SqlProvider } from "@zxteam/sql";
import { CancellationToken } from "@zxteam/contract";

export class PostgresMigrationManager extends MigrationManager {

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
			"SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' AND tablename = $1"
		).executeScalarOrNull(cancellationToken, this.versionTableName);

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
		const tableCountData = await sqlProvider.statement(
			"SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' AND tablename != 'emptytestflag'"
		).executeScalar(cancellationToken);
		if (tableCountData.asString !== "0") { // asString because BigInt
			throw new PostgresMigrationManager.MigrationError("Your database has tables. Create Version Table allowed only for an empty database. Please create Version Table yourself.");
		}

		const viewsCountData = await sqlProvider.statement(
			"SELECT COUNT(*) FROM pg_catalog.pg_views WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
		).executeScalar(cancellationToken);
		if (viewsCountData.asString !== "0") { // asString because BigInt
			throw new PostgresMigrationManager.MigrationError("Your database has views. Create Version Table allowed only for an empty database. Please create Version Table yourself.");
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
