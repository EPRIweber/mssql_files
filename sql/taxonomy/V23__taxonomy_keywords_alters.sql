DROP TABLE course_taxonomy;
DROP TABLE course_skills;
DROP TABLE skills;
DROP TABLE taxonomy;






CREATE TABLE keywords01 (
    keyword_id NVARCHAR(50) PRIMARY KEY NOT NULL,
    keyword_description NVARCHAR(MAX) DEFAULT NULL
);

CREATE TABLE taxonomy01 (
    taxonomy_id  NVARCHAR(50) PRIMARY KEY NOT NULL,
    taxonomy_description NVARCHAR(MAX) DEFAULT NULL
);

CREATE TABLE course_keywords01 (
    course_id UNIQUEIDENTIFIER NOT NULL,
    keyword_id NVARCHAR(50) NOT NULL,

    -- Composite primary key to ensure each course-keyword pair is unique
    CONSTRAINT pk_course_keywords PRIMARY KEY (course_id, keyword_id),

    -- Foreign key to the courses table
    CONSTRAINT fk_course_keywords_course FOREIGN KEY (course_id) 
        REFERENCES dbo.courses (course_id)
        ON DELETE CASCADE, -- If a course is deleted, its keyword relationships are also deleted

    -- Foreign key to the keywords table
    CONSTRAINT fk_course_keywords_keyword FOREIGN KEY (keyword_id) 
        REFERENCES dbo.keywords01 (keyword_id)
        ON DELETE CASCADE -- If a keyword is deleted, its course relationships are also deleted
);

CREATE TABLE course_taxonomy01 (
    course_id UNIQUEIDENTIFIER NOT NULL,
    taxonomy_id NVARCHAR(50) NOT NULL,

    -- Composite primary key to ensure each course-taxonomy pair is unique
    CONSTRAINT pk_course_taxonomy PRIMARY KEY (course_id, taxonomy_id),

    -- Foreign key to the courses table
    CONSTRAINT fk_course_taxonomy_course FOREIGN KEY (course_id) 
        REFERENCES dbo.courses (course_id)
        ON DELETE CASCADE, -- If a course is deleted, its taxonomy relationships are also deleted

    -- Foreign key to the taxonomy table
    CONSTRAINT fk_course_taxonomy_taxonomy FOREIGN KEY (taxonomy_id) 
        REFERENCES dbo.taxonomy01 (taxonomy_id)
        ON DELETE CASCADE -- If a taxonomy item is deleted, its course relationships are also deleted
);