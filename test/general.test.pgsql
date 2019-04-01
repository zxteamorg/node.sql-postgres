-- DROP TABLE IF EXISTS "tb_1";
CREATE TABLE "tb_1" (
	"varchar" VARCHAR(128) NOT NULL,
	"int" INT NOT NULL,
	UNIQUE ("varchar"),
	UNIQUE ("int")
);
INSERT INTO "tb_1" VALUES ('one', 1);
INSERT INTO "tb_1" VALUES ('two', 2);
INSERT INTO "tb_1" VALUES ('three', 3);


-- DROP FUNCTION IF EXISTS sp_contains;
CREATE FUNCTION sp_contains("value" VARCHAR(128))
	RETURNS bool
	LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
	RETURN(SELECT EXISTS(
		SELECT 1
		FROM "tb_1" AS t
		WHERE t."varchar" = "value"
	) AS "is_exist");
END;
$BODY$;


-- DROP FUNCTION IF EXISTS sp_empty_fetch;
CREATE FUNCTION sp_empty_fetch()
	RETURNS void
	LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
END;
$BODY$;


-- DROP FUNCTION IF EXISTS sp_single_fetch;
CREATE FUNCTION sp_single_fetch()
	RETURNS TABLE (
	"varchar" VARCHAR(128),
	"int" INT
	)
	LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
	RETURN QUERY SELECT * FROM "tb_1";
END;
$BODY$;


-- DROP FUNCTION IF EXISTS sp_multi_fetch;
CREATE FUNCTION sp_multi_fetch()
	RETURNS SETOF refcursor
	LANGUAGE plpgsql
AS $BODY$
DECLARE
	result1 refcursor;
	result2 refcursor;
BEGIN
	OPEN result1 FOR SELECT * FROM "tb_1";
	RETURN NEXT result1;

	OPEN result2 FOR SELECT * FROM "tb_1";
	RETURN NEXT result2;
END;
$BODY$;


-- DROP FUNCTION IF EXISTS sp_multi_fetch_ints;
CREATE FUNCTION sp_multi_fetch_ints()
	RETURNS SETOF refcursor
	LANGUAGE plpgsql
AS $BODY$
DECLARE
	result1 refcursor;
	result2 refcursor;
BEGIN
	OPEN result1 FOR SELECT "int" FROM "tb_1";
	RETURN NEXT result1;

	OPEN result2 FOR SELECT "int" FROM "tb_1";
	RETURN NEXT result2;
END;
$BODY$;
