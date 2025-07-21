-- V34__skip_dbt_tmp_in_trg_after_create_table.sql
-------------------------------------------------------------------------------
/* Update trigger so it ignores dbt’s __dbt_tmp / __dbt_backup tables         */
CREATE OR ALTER TRIGGER trg_after_create_table
ON DATABASE
AFTER CREATE_TABLE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ev  xml     = EVENTDATA();
    DECLARE @sch sysname = @ev.value('(/EVENT_INSTANCE/SchemaName)[1]', 'sysname');
    DECLARE @tbl sysname = @ev.value('(/EVENT_INSTANCE/ObjectName)[1]', 'sysname');

    /* ----  NEW GUARD CLAUSE  -------------------------------------------- */
    IF @tbl LIKE '%[_][_][d][b][t][_][t][m][p]'   -- …__dbt_tmp
       OR @tbl LIKE '%[_][d][b][t][_][b][a][c][k][u][p]'  -- …_dbt_backup
       RETURN;
    /* -------------------------------------------------------------------- */

    IF @sch NOT IN ('sys', 'INFORMATION_SCHEMA')
        EXEC dbo.add_audit_columns @sch, @tbl;
END;
GO
