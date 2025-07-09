/* ---------------------------------------------------------------------------
   Migration: V35__taxonomy01_add_hydrogen_and_why.sql
   Purpose  : Add a “hydrogen_related” flag (BIT, default FALSE)
              and a capped “why” description (NVARCHAR(255)) to taxonomy01.
--------------------------------------------------------------------------- */

ALTER TABLE dbo.taxonomy01
ADD hydrogen_related BIT NOT NULL
        CONSTRAINT DF_taxonomy01_hydrogen_related DEFAULT (0),
    why NVARCHAR(255) NULL;
