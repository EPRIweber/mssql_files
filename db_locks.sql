CREATE OR ALTER PROCEDURE dbo.begin_run
AS
/*  Start Scraping Job "Lock"  */
BEGIN
    SET NOCOUNT ON;
    BEGIN TRAN;
        IF EXISTS (SELECT 1 FROM runs WHERE run_status = 'running')
        BEGIN
            SELECT CAST(NULL AS INT) AS run_id;      -- someone else is running
            ROLLBACK TRAN;
            RETURN;
        END

        INSERT runs(run_status) VALUES('running');
        DECLARE @id INT = SCOPE_IDENTITY();
        COMMIT TRAN;

    SELECT @id AS run_id;
END;
GO

CREATE OR ALTER PROCEDURE dbo.end_run
(
    @run_id INT
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE runs
       SET run_status = 'finished',
           run_end_time = SYSUTCDATETIME()
     WHERE run_id = @run_id;
END;
GO
