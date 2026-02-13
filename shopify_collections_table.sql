IF OBJECT_ID('dbo.Shopify_Collections', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Shopify_Collections (
        CollectionId BIGINT NULL,
        CollectionGid NVARCHAR(128) NOT NULL,
        Title NVARCHAR(255) NOT NULL,
        Handle NVARCHAR(255) NULL,
        UpdatedAt DATETIME2 NULL,
        Shop NVARCHAR(255) NOT NULL,
        ExportedAt DATETIME2 NOT NULL
    );
END;
