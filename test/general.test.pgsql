DROP TABLE IF EXISTS "tb_1";
CREATE TABLE "tb_1" (
	"varchar" VARCHAR(128) NOT NULL,
	"int" INT NOT NULL,
	UNIQUE ("varchar"),
	UNIQUE ("int")
);
INSERT INTO "tb_1" VALUES ('one', 1);
INSERT INTO "tb_1" VALUES ('two', 2);
INSERT INTO "tb_1" VALUES ('three', 3);



-- DROP FUNCTION IF EXISTS sp_contains();
CREATE OR REPLACE FUNCTION sp_contains(nameColnm VARCHAR(128))
	RETURNS bool
	LANGUAGE 'plpgsql'
AS $BODY$

BEGIN
	RETURN(SELECT EXISTS(
		SELECT 1
		FROM "tb_1" AS t
		WHERE t."varchar" = nameColnm
	) AS "is_exist");
END;
$BODY$;

-- DROP FUNCTION IF EXISTS sp_empty_fetch();
CREATE OR REPLACE FUNCTION sp_empty_fetch()
	RETURNS void
	LANGUAGE 'plpgsql'
AS $BODY$

BEGIN
END;
$BODY$;

CREATE OR REPLACE FUNCTION sp_single_fetch()
	RETURNS TABLE (
	"varchar" VARCHAR(128),
	"int" INT
	)
	LANGUAGE 'plpgsql'
AS $BODY$

BEGIN
	RETURN QUERY
		SELECT * FROM "tb_1";
END;
$BODY$;

-- CREATE OR REPLACE FUNCTION sp_multi_fetch()
-- 	RETURNS TABLE (
-- 	"varchar" VARCHAR(128),
-- 	"int" INT
-- 	)
-- 	LANGUAGE 'plpgsql'
-- AS $BODY$

-- BEGIN
-- 	RETURN QUERY
-- 		SELECT * FROM "tb_1"
-- 		SELECT * FROM "tb_1";
-- END;
-- $BODY$;

-- CREATE OR REPLACE FUNCTION sp_multi_fetch_ints()
-- 	RETURNS TABLE (
-- 	"int" INT
-- 	)
-- 	LANGUAGE 'plpgsql'
-- AS $BODY$

-- BEGIN
-- 	RETURN QUERY
-- 		SELECT "int" FROM "tb_1"
-- 		SELECT "int" FROM "tb_1";
-- END;
-- $BODY$;
